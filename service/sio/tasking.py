import hashlib
import json
import random
import threading

from flask import request
from flask_socketio import Namespace

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.remote.datatypes.queues.named import NamedQueue
from service.config import LOGGER

config = forge.get_config()
datastore = forge.get_datastore()
filestore = forge.get_filestore()

dispatch_client = DispatchClient(datastore)


class TaskingNamespace(Namespace):
    def __init__(self, namespace=None):
        self.connections_lock = threading.RLock()
        self.client_map = {}
        self.watch_threads = set()
        self.banned_clients = []
        super().__init__(namespace=namespace)

    def _deactivate_client(self, client_id):
        with self.connections_lock:
            if client_id in self.banned_clients:
                self.banned_clients.remove(client_id)

            for svc_name in list(self.client_map.keys()):
                if client_id in self.client_map[svc_name]:
                    self.client_map[svc_name].remove(client_id)
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Done waiting for {svc_name} tasks")

    def on_service_client_connect(self):
        client_id = get_request_id(request)
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - New connection established from: {ip}")

    def on_disconnect(self):
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        client_id = get_request_id(request)
        self._deactivate_client(client_id)

        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Disconnected from: {ip}")

    def default_error_handler(self, e):
        LOGGER.info(f"Error: {str(e)}")
        pass

    # noinspection PyBroadException
    def get_task_for_service(self, service_name, service_version, service_tool_version):
        with self.connections_lock:
            if service_name in self.watch_threads:
                return
            self.watch_threads.add(service_name)

        LOGGER.info(f"SocketIO:{self.namespace} - Starting to monitor {service_name} queue for new tasks")
        queue = NamedQueue(service_queue_name(service_name), private=True)
        counter = MetricsFactory('service', name=service_name, config=config)

        try:
            while True:
                task = queue.pop(timeout=1)
                with self.connections_lock:
                    clients = list(set(self.client_map.get(service_name, [])).difference(set(self.banned_clients)))
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
                    self.socketio.emit("got_task", task.as_primitives(), namespace=self.namespace, room=client_id)
                    with self.connections_lock:
                        self.banned_clients.append(client_id)

                    dispatch_client.running_tasks.set(task.key(), task.as_primitives())
                    LOGGER.info(f"SocketIO:{self.namespace} - Sending {service_name} task to client {client_id}")
                else:
                    dispatch_client.service_finished(task.sid, result_key, result)

        except Exception:
            LOGGER.exception(f"SocketIO:{self.namespace}")
        finally:
            if service_name in self.watch_threads:
                self.watch_threads.remove(service_name)

            LOGGER.info(f"SocketIO:{self.namespace} - No more clients connected to service "
                        f"{service_name} queue, exiting thread...")

    def on_done_task(self, service_name, exec_time, task, result):
        client_id = get_request_id(request)
        counter = MetricsFactory('service', name=service_name, config=config)
        counter_timing = MetricsFactory('service', name=service_name, config=config)

        task = Task(task)
        expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
        result['expiry_ts'] = expiry_ts

        if 'result' in result:  # Task completed successfully
            LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Client successfully completed the {service_name} task in {exec_time}ms")

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
            LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Client failed to complete the {service_name} task in {exec_time}ms")

            error = Error(result)

            service_tool_version_hash = hashlib.md5((error.response.service_tool_version.encode('utf-8'))).hexdigest()
            task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
            conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

            error_key = error.build_key(conf_key)
            dispatch_client.service_failed(task.sid, error_key, error)

            # Metrics
            if error.response.status == "FAIL_RECOVERABLE":
                counter.increment('fail_recoverable')
            else:
                counter.increment('fail_nonrecoverable')

        counter_timing.increment_execution_time('execution', exec_time)

    def on_got_task(self, service_name, idle_time):
        counter_timing = MetricsFactory('service', name=service_name, config=config)
        counter_timing.increment_execution_time('idle', idle_time)
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Client was idle for {idle_time}ms and received the {service_name} task and started processing")
        self._deactivate_client(client_id)

    def on_wait_for_task(self, service_name, service_version, service_tool_version):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Waiting for tasks in {service_name}[{service_version}] queue...")

        with self.connections_lock:
            if service_name not in self.client_map:
                self.client_map[service_name] = []
            self.client_map[service_name].append(client_id)

        self.socketio.start_background_task(target=self.get_task_for_service, service_name=service_name, service_version=service_version, service_tool_version=service_tool_version)
        self.socketio.emit('wait_for_task', client_id, namespace=self.namespace, room=client_id)


def get_request_id(request_p):
    if hasattr(request_p, "sid"):
        return request_p.sid
    return None
