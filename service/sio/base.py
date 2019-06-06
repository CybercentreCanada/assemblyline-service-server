import functools
import logging
import threading

from flask import request
from flask_socketio import Namespace, disconnect

from assemblyline.common import forge
from assemblyline.common.str_utils import StringTable
from assemblyline.remote.datatypes.hash import Hash
from service.config import AUTH_KEY

config = forge.get_config()

KV_SESSION = Hash('flask_sessions',
                  host=config.core.redis.nonpersistent.host,
                  port=config.core.redis.nonpersistent.port,
                  db=config.core.redis.nonpersistent.db)

LOGGER = logging.getLogger('assemblyline.svc.socketio')

STATUS = StringTable('STATUS', [
    ('INITIALIZING', 0),
    ('WAITING', 1),
    ('PROCESSING', 2),
    ('RESULT_FOUND', 3),
    ('ERROR_FOUND', 4),
])

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
        self.clients = {}
        self.available_clients = {}
        self.banned_clients = []
        super().__init__(namespace=namespace)

    def _activate_client(self, client_info):
        with self.connections_lock:
            if client_info['service_name'] not in self.available_clients:
                self.available_clients[client_info['service_name']] = []
            self.available_clients[client_info['service_name']].append(client_info['id'])


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
            self.clients[client_info['id']] = client_info

        LOGGER.info(f"SocketIO:{self.namespace} - {client_info['id']} - "
                    f"New connection established from: {client_info['ip']}")

    def on_disconnect(self):
        client_id = get_request_id(request)
        self._deactivate_client(client_id)

        with self.connections_lock:
            if client_id in self.clients:
                client_info = self.clients[client_id]
                LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                            f"Client disconnected from: {client_info['ip']}")

            self.clients.pop(client_id, None)


def get_request_id(request_p):
    if hasattr(request_p, 'sid'):
        return request_p.sid
    return None


def get_client_info(request_p):
    client_id = get_request_id(request_p)
    src_ip = request_p.headers.get('X-Forwarded-For', request_p.remote_addr)
    auth_key = request_p.headers.get('Service-API-Auth-Key', None)
    if AUTH_KEY != auth_key:
        raise AuthenticationFailure(f"Client key does not match server key. Connection refused from: {src_ip}")

    service_name = request_p.headers.get('Service-Name', None)
    service_version = request_p.headers.get('Service-Version', None)
    service_tool_version = request_p.headers.get('Service-Tool-Version', None)

    return {
        'id': client_id,
        'ip': src_ip,
        'service_name': service_name,
        'service_version': service_version,
        'service_tool_version': service_tool_version,
        'status': STATUS.INITIALIZING
    }
