import hashlib
import json
import random
import threading
import time


from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.service_timing_heartbeat import Metrics as TimingMetrics
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.remote.datatypes.queues.named import NamedQueue
from service.sio.base import BaseNamespace, authenticated_only, LOGGER, get_request_id, request

config = forge.get_config()
datastore = forge.get_datastore()
filestore = forge.get_filestore()


class TaskingNamespace(BaseNamespace):
    def __init__(self, namespace=None):
        self.watch_threads = set()
        self.dispatch_client = DispatchClient(datastore)
        super().__init__(namespace=namespace)

        # TODO remove client from _report_times, _counters when it disconnects
        self._report_times_lock = threading.Lock()
        self._report_times = {}
        self._counters = {}
        thread = threading.Thread(target=self._do_reports_between_events)
        thread.daemon = True
        thread.start()

    def on_disconnect(self):
        super().on_disconnect()
        client_id = get_request_id(request)
        with self._report_times_lock:
            if client_id in self._counters:
                self._counters[client_id][0].stop()
                self._counters[client_id][1].stop()
            self._counters.pop(client_id, None)
            self._report_times.pop(client_id, None)

    def _get_counters(self, client_id, service_name):
        if client_id not in self._counters:
            self._counters[client_id] = (
                MetricsFactory('service_timing', TimingMetrics, name=service_name, config=config),
                MetricsFactory('service', Metrics, name=service_name, config=config)
            )
        return self._counters[client_id]

    def _do_reports_between_events(self):
        while True:
            time.sleep(0.1)
            with self.connections_lock:
                for client_info in list(self.clients.values()):
                    if client_info['id'] in self.banned_clients:
                        self.report_active(client_info)
                    else:
                        self.report_idle(client_info)

    def report_idle(self, client_info):
        with self._report_times_lock:
            now = time.time()
            delta = now - self._report_times.get(client_info['id'], now)
            self._report_times[client_info['id']] = now
            _, counter_timing = self._get_counters(client_info['id'], client_info['service_name'])
            counter_timing.increment_execution_time('idle', delta)

    def report_active(self, client_info):
        with self._report_times_lock:
            now = time.time()
            delta = now - self._report_times.get(client_info['id'], now)
            self._report_times[client_info['id']] = now
            _, counter_timing = self._get_counters(client_info['id'], client_info['service_name'])
            counter_timing.increment_execution_time('execution', delta)

    # noinspection PyBroadException
    def get_task_for_service(self, client_info):
        service_name = client_info['service_name']
        service_version = client_info['service_version']
        service_tool_version = client_info['service_tool_version']

        with self.connections_lock:
            if service_name in self.watch_threads:
                return
            self.watch_threads.add(service_name)

        LOGGER.info(f"SocketIO:{self.namespace} - Starting to monitor {service_name} queue for new tasks")
        queue = NamedQueue(service_queue_name(service_name), private=True)
        counter, _ = self._get_counters(client_info['id'], service_name)

        try:
            while True:
                task = self.dispatch_client.request_work(service_name, timeout=1)
                with self.connections_lock:
                    clients = list(set(self.available_clients.get(service_name, [])).difference(set(self.banned_clients)))
                    if len(clients) == 0:
                        # We have no more client, put the task back and quit...
                        if task:
                            queue.push(task.as_primitives())
                        break

                if not task:
                    continue

                counter.increment('execute')

                service_tool_version_hash = hashlib.md5((service_tool_version.encode('utf-8'))).hexdigest()
                task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

                result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                                   service_name=service_name,
                                                   service_version=service_version,
                                                   conf_key=conf_key)

                result = datastore.result.get_if_exists(result_key)
                if not result:
                    counter.increment('cache_miss')

                    client_id = random.choice(clients)
                    self.socketio.emit('got_task', task.as_primitives(), namespace=self.namespace, room=client_id)
                    with self.connections_lock:
                        self.banned_clients.append(client_id)

                    LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                                f"Sending {service_name} service task to client")
                else:
                    self.dispatch_client.service_finished(task.sid, result_key, result)

        except Exception:
            LOGGER.exception(f"SocketIO:{self.namespace}")
        finally:
            if service_name in self.watch_threads:
                self.watch_threads.remove(service_name)

            LOGGER.info(f"SocketIO:{self.namespace} - No more clients connected to "
                        f"{service_name} service queue, exiting thread...")

    @authenticated_only
    def on_done_task(self, exec_time, task, result, client_info):
        service_name = client_info['service_name']
        counter, counter_timing = self._get_counters(client_info['id'], service_name)
        # counter = MetricsFactory('service', Metrics, name=service_name, config=config)
        # counter_timing = MetricsFactory('service_timing', TimingMetrics, name=service_name, config=config)

        task = Task(task)
        expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
        result['expiry_ts'] = expiry_ts

        if 'result' in result:  # Task completed successfully
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                        f"Client successfully completed the {service_name} task in {exec_time}ms")

            result = Result(result)

            service_tool_version_hash = hashlib.md5((result.response.service_tool_version.encode('utf-8'))).hexdigest()
            task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
            conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
            result_key = result.build_key(conf_key)
            self.dispatch_client.service_finished(task.sid, result_key, result)

            # Metrics
            if result.result.score > 0:
                counter.increment('scored')
            else:
                counter.increment('not_scored')
        else:  # Task failed
            LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                        f"Client failed to complete the {service_name} task in {exec_time}ms")

            error = Error(result)

            service_tool_version_hash = hashlib.md5((error.response.service_tool_version.encode('utf-8'))).hexdigest()
            task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
            conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

            error_key = error.build_key(conf_key)
            self.dispatch_client.service_failed(task.sid, error_key, error)

            # Metrics
            if error.response.status == 'FAIL_RECOVERABLE':
                counter.increment('fail_recoverable')
            else:
                counter.increment('fail_nonrecoverable')

        # counter_timing.increment_execution_time('execution', exec_time)
        self.report_active(client_info)

    @authenticated_only
    def on_got_task(self, idle_time, client_info):
        service_name = client_info['service_name']
        # counter_timing = MetricsFactory('service_timing', TimingMetrics, name=service_name, config=config)
        # counter_timing.increment_execution_time('idle', idle_time)
        self.report_idle(client_info)

        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Client was idle for {idle_time}ms and received the {service_name} task and started processing")
        self._deactivate_client(client_info['id'])

    @authenticated_only
    def on_wait_for_task(self, client_info):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Waiting for tasks in {client_info['service_name']} service queue...")

        self._activate_client(client_info)

        self.socketio.start_background_task(target=self.get_task_for_service, client_info=client_info)
