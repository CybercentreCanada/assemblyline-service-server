import random
import threading

from flask import request
from flask_socketio import emit, Namespace

from al_core.dispatching.dispatcher import service_queue_name
from service.config import LOGGER
from assemblyline.remote.datatypes.queues.named import NamedQueue


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


    def on_connect(self):
        client_id = get_request_id(request)
        ip = request.headers.get("X-Forward-For", request.remote_addr)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - New connection establish from: {ip}")

    def on_disconnect(self):
        ip = request.headers.get("X-Forward-For", request.remote_addr)
        client_id = get_request_id(request)
        self._deactivate_client(client_id)

        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Disconnected from: {ip}")

    # noinspection PyBroadException
    def get_task_for_service(self, service_name):
        with self.connections_lock:
            if service_name in self.watch_threads:
                return
            self.watch_threads.add(service_name)

        LOGGER.info(f"SocketIO:{self.namespace} - Starting to monitor {service_name} queue for new tasks")
        q = NamedQueue(service_queue_name(service_name), private=True)
        try:
            while True:
                task = q.pop(timeout=1)
                with self.connections_lock:
                    clients = list(set(self.client_map.get(service_name, [])).difference(set(self.banned_clients)))
                    if len(clients) == 0:
                        # We have no more client, put the task back and quit...
                        if task:
                            q.push(task)
                        break

                if not task:
                    continue

                client_id = random.choice(clients)
                self.socketio.emit("got_task", task, namespace=self.namespace, room=client_id)
                with self.connections_lock:
                    self.banned_clients.append(client_id)

                LOGGER.info(f"SocketIO:{self.namespace} - Sending {service_name} task to client {client_id}")

        except Exception:
            LOGGER.exception(f"SocketIO:{self.namespace}")
        finally:
            if service_name in self.watch_threads:
                self.watch_threads.remove(service_name)

            LOGGER.info(f"SocketIO:{self.namespace} - No more clients connected to service "
                        f"{service_name} queue, exiting thread...")

    def on_got_task(self):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Client received the task and started processing")
        self._deactivate_client(client_id)

    def on_wait_for_task(self, service_name, version):
        client_id = get_request_id(request)
        LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - Waiting tasks in {service_name}[{version}] queue...")

        with self.connections_lock:
            if service_name not in self.client_map:
                self.client_map[service_name] = []
            self.client_map[service_name].append(client_id)

        self.socketio.start_background_task(target=self.get_task_for_service, service_name=service_name)
        emit('wait_for_task', (service_name, version))

def get_request_id(request_p):
    if hasattr(request_p, "sid"):
        return request_p.sid
    return None
