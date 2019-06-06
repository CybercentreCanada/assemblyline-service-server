import hashlib
import json
import random

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
from service.sio.base import BaseNamespace, authenticated_only, LOGGER

config = forge.get_config()
datastore = forge.get_datastore()
filestore = forge.get_filestore()

dispatch_client = DispatchClient(datastore)


class TaskingNamespace(BaseNamespace):
    def __init__(self, namespace=None):
        self.watch_threads = set()
        super().__init__(namespace=namespace)

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
        counter = MetricsFactory('service', Metrics, name=service_name, config=config)

        try:
            while True:
                task = queue.pop(timeout=1)
                with self.connections_lock:
                    clients = list(set(self.available_clients.get(service_name, [])).difference(set(self.banned_clients)))
                    if len(clients) == 0:
                        # We have no more client, put the task back and quit...
                        if task:
                            queue.push(task)
                        break

                if not task:
                    continue

                task = Task(task)
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

                    dispatch_client.running_tasks.set(task.key(), task.as_primitives())
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                                f"Sending {service_name} task to client")
                else:
                    dispatch_client.service_finished(task.sid, result_key, result)

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
        counter = MetricsFactory('service', Metrics, name=service_name, config=config)
        counter_timing = MetricsFactory('service', TimingMetrics, name=service_name, config=config)

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
            dispatch_client.service_finished(task.sid, result_key, result)

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
            dispatch_client.service_failed(task.sid, error_key, error)

            # Metrics
            if error.response.status == 'FAIL_RECOVERABLE':
                counter.increment('fail_recoverable')
            else:
                counter.increment('fail_nonrecoverable')

        counter_timing.increment_execution_time('execution', exec_time)

    @authenticated_only
    def on_got_task(self, idle_time, client_info):
        service_name = client_info['service_name']
        counter_timing = MetricsFactory('service', TimingMetrics, name=service_name, config=config)
        counter_timing.increment_execution_time('idle', idle_time)

        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Client was idle for {idle_time}ms and received the {service_name} task and started processing")
        self._deactivate_client(client_info['id'])

    @authenticated_only
    def on_wait_for_task(self, client_info):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"Waiting for tasks in {client_info['service_name']} service queue...")

        self._activate_client(client_info)

        self.socketio.start_background_task(target=self.get_task_for_service, client_info=client_info)
