from unittest import mock

import uuid
import hashlib
import time
import pytest
from assemblyline.remote.datatypes import get_client
from flask import Flask
import flask_socketio


from assemblyline.odm import randomizer
from assemblyline.common import forge
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_service_server.config import AUTH_KEY
from assemblyline_service_server.sio.tasking import TaskingNamespace
from assemblyline_core.dispatching.dispatcher import ServiceTask
from assemblyline_core.dispatching.dispatcher import service_queue_name

SECRET_KEY = uuid.uuid4().hex


class RedisTime:
    def __init__(self):
        self.current = None

    def __call__(self):
        if self.current is not None:
            return self.current, 0
        return time.time(), 0


@pytest.fixture(scope='function')
def redis():
    config = forge.get_config()
    client = get_client(
        config.core.metrics.redis.host,
        config.core.metrics.redis.port,
        8,
        False
    )
    client.flushdb()
    yield client
    client.flushdb()


service_name = 'Extract'


@pytest.fixture(scope='function')
def ds():
    ds = forge.get_datastore()
    ds.service_delta.update(service_name, [
        (ds.service_delta.UPDATE_SET, 'enabled', True)
    ])
    return ds


@pytest.fixture(scope='function')
def tasking_namespace(redis):
    tn = TaskingNamespace('/tasking', redis)
    try:
        yield tn
    finally:
        tn.running = False


@pytest.fixture()
def headers():
    return {
        'Container-Id': randomizer.get_random_hash(12),
        'Service-API-Auth-Key': AUTH_KEY,
        'Service-Name': service_name,
        'Service-Version': randomizer.get_random_service_version(),
        'Service-Tool-Version': randomizer.get_random_hash(64),
        'Service-Timeout': str(300),
        'X-Forwarded-For': '127.0.0.1',
    }


@pytest.fixture(scope="function")
def tasking(redis, tasking_namespace, headers):
    # Create our own flask and socketio so we can control redis for mocking
    app = Flask('svc-socketio')
    app.config['SECRECT_KEY'] = SECRET_KEY
    socketio = flask_socketio.SocketIO(app, async_mode='threading')

    # Loading the different namespaces
    # socketio.on_namespace(HelperNamespace('/helper'))
    socketio.on_namespace(tasking_namespace)

    client = flask_socketio.SocketIOTestClient(app, socketio, namespace='/tasking', headers=headers)
    yield client
    tasking_namespace.running = False
    if client.is_connected('/tasking'):
        client.disconnect('/tasking')


def test_connect_disconnect(tasking, tasking_namespace, headers):
    assert len(tasking_namespace.clients) == 1
    client_data = list(tasking_namespace.clients.values())[0]
    assert headers['Container-Id'] == client_data.container_id
    assert headers['Service-Name'] == client_data.service_name
    tasking.disconnect('/tasking')
    assert len(tasking_namespace.clients) == 0


def test_get_task(redis, tasking, headers, tasking_namespace):

    client_id, client_data = list(tasking_namespace.clients.items())[0]
    assert headers['Container-Id'] == client_data.container_id
    assert service_name == client_data.service_name

    tasking.emit('wait_for_task', namespace='/tasking')

    assert client_id in tasking_namespace.available_clients[service_name]

    work_queue = NamedQueue(service_queue_name(service_name), host=redis)
    work_queue.push(ServiceTask({
        'fileinfo': {
            'magic': 'file',
            'md5': hashlib.md5(b'test_get_task').hexdigest(),
            'mime': 'file',
            'sha1': hashlib.sha1(b'test_get_task').hexdigest(),
            'sha256': hashlib.sha256(b'test_get_task').hexdigest(),
            'size': 100,
            'type': 'unknown',
        },
        'service_name': service_name,
        'max_files': 10,
        'ttl': 10,
    }).as_primitives())

    start = time.time()
    message = None
    while time.time() - start < 10 and message is None:
        for msg in tasking.get_received('/tasking'):
            message = msg
        else:
            time.sleep(0.1)

    assert client_id in tasking_namespace.banned_clients
    assert message['name'] == 'got_task'


def test_flush_on_disable(tasking_namespace, ds, redis):

    dc = tasking_namespace.dispatch_client.service_failed = mock.MagicMock()

    work_queue = NamedQueue(service_queue_name(service_name), host=redis)
    for ii in range(10):
        body = b'abc' + str(ii).encode()
        work_queue.push(ServiceTask({
            'fileinfo': {
                'magic': 'file',
                'md5': hashlib.md5(body).hexdigest(),
                'mime': 'file',
                'sha1': hashlib.sha1(body).hexdigest(),
                'sha256': hashlib.sha256(body).hexdigest(),
                'size': 100,
                'type': 'unknown',
            },
            'service_name': service_name,
            'max_files': 10,
            'ttl': 10,
        }).as_primitives())

    ds.service_delta.update(service_name, [
        (ds.service_delta.UPDATE_SET, 'enabled', False)
    ])

    start = time.time()
    while time.time() - start < 120 and work_queue.length() != 0:
        time.sleep(0.1)

    assert dc.call_count >= 10

    ds.service_delta.update(service_name, [
        (ds.service_delta.UPDATE_SET, 'enabled', True)
    ])


#
# def test_send_result():
#     raise NotImplementedError()
#
#
# def test_timeout():
#     raise NotImplementedError()
#
