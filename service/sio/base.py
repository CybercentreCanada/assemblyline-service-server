import logging
import threading

from flask import request
from flask_socketio import Namespace

from assemblyline.common import forge
from assemblyline.remote.datatypes.hash import Hash

config = forge.get_config()

KV_SESSION = Hash('flask_sessions',
                  host=config.core.redis.nonpersistent.host,
                  port=config.core.redis.nonpersistent.port,
                  db=config.core.redis.nonpersistent.db)

LOGGER = logging.getLogger('assemblyline.svc.socketio')

class AuthenticationFailure(Exception):
    pass


class BaseNamespace(Namespace):
    def __init__(self, namespace=None):
        self.connections_lock = threading.RLock()
        self.clients = {}
        self.banned_clients = []
        super().__init__(namespace=namespace)

    def _deactivate_client(self, sid):
        with self.connections_lock:
            if sid in self.banned_clients:
                self.banned_clients.remove(sid)

            for svc_name in list(self.clients.keys()):
                if sid in self.clients[svc_name]:
                    self.clients[svc_name].remove(sid)
                    LOGGER.info(f"SocketIO:{self.namespace} - {sid} - "
                                f"Done waiting for {svc_name} tasks")

    def on_connect(self):
        try:
            info = get_client_info(request)
        except AuthenticationFailure as e:
            LOGGER.warning(str(e))
            return

        sid = get_request_id(request)

        with self.connections_lock:
            self.clients[sid] = info

        LOGGER.info(f"SocketIO:{self.namespace} - {info['sid']} - "
                    f"New connection established from: {info['ip']}")

    def on_disconnect(self):
        sid = get_request_id(request)
        self._deactivate_client(sid)
        with self.connections_lock:
            if sid in self.clients:
                info = self.clients[get_request_id(request)]
                LOGGER.info(f"SocketIO:{self.namespace} - {info['sid']} - "
                            f"Client disconnected from: {info['ip']}")

            self.clients.pop(sid, None)


def get_request_id(request_p):
    if hasattr(request_p, 'sid'):
        return request_p.sid
    return None


def get_client_info(request_p):

    src_ip = request_p.headers.get('X-Forwarded-For', request_p.remote_addr)
    sid = get_request_id(request_p)

    return {
        'ip': src_ip,
        'sid': sid
    }
