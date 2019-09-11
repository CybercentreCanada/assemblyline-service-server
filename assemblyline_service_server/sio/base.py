import functools
import logging
import threading
from typing import Dict, List

from flask import request
from flask_socketio import Namespace, disconnect

from assemblyline.common import forge
from assemblyline_service_server.config import AUTH_KEY
from assemblyline_service_server.session import ServiceClient

config = forge.get_config()

LOGGER = logging.getLogger('assemblyline.svc.socketio')


class AuthenticationFailure(Exception):
    pass


def authenticated_only(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        self = args[0]
        client_info = self.clients.get(get_request_id(request), None)
        if not client_info:
            disconnect()
        else:
            kwargs['client_info'] = client_info
            return f(*args, **kwargs)

    return wrapped


class BaseNamespace(Namespace):
    def __init__(self, namespace=None):
        self.connections_lock = threading.RLock()
        self.clients: Dict[str, ServiceClient] = {}
        self.available_clients: Dict[str, List[str]] = {}
        self.banned_clients: List[str] = []
        super().__init__(namespace=namespace)

    def _activate_client(self, client_info: ServiceClient):
        with self.connections_lock:
            if client_info.service_name not in self.available_clients:
                self.available_clients[client_info.service_name] = []
            self.available_clients[client_info.service_name].append(client_info.client_id)

    def _deactivate_client(self, client_id):
        with self.connections_lock:
            if client_id in self.banned_clients:
                self.banned_clients.remove(client_id)

            for service_name in list(self.available_clients.keys()):
                if client_id in self.available_clients[service_name]:
                    self.available_clients[service_name].remove(client_id)
                    LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                                f"Done waiting for {service_name} tasks")

    def on_connect(self):
        try:
            client_info = get_client_info(request)
        except AuthenticationFailure as e:
            LOGGER.warning(str(e))
            return

        with self.connections_lock:
            self.clients[client_info.client_id] = client_info

        LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                    f"New connection established from: {client_info.ip}")

    def on_disconnect(self):
        client_id = get_request_id(request)
        self._deactivate_client(client_id)

        with self.connections_lock:
            if client_id in self.clients:
                client_info = self.clients[client_id]
                LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                            f"Client disconnected from: {client_info.ip}")

            self.clients.pop(client_id, None)


def get_request_id(request_p):
    if hasattr(request_p, 'sid'):
        return request_p.sid
    return None  # TODO should this branch be here?


def get_client_info(request_p) -> ServiceClient:
    client_id = get_request_id(request_p)
    src_ip = request_p.headers.get('X-Forwarded-For', request_p.remote_addr)
    auth_key = request_p.headers.get('Service-API-Auth-Key', None)
    if AUTH_KEY != auth_key:
        raise AuthenticationFailure(f"Client key does not match server key. Connection refused from: {src_ip}")

    container_id = request_p.headers['Container-Id']
    service_name = request_p.headers['Service-Name']
    service_version = request_p.headers['Service-Version']
    service_tool_version = request_p.headers.get('Service-Tool-Version', None)
    service_timeout = request_p.headers['Service-Timeout']

    return ServiceClient(dict(
        client_id=client_id,
        container_id=container_id,
        ip=src_ip,
        service_name=service_name,
        service_version=service_version,
        service_tool_version=service_tool_version,
        service_timeout=service_timeout,
    ))
