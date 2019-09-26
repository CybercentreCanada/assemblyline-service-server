from unittest.mock import patch, MagicMock

import pytest

from assemblyline.odm.models.result import Result
from assemblyline.odm.models.error import Error
from assemblyline.odm.messages.task import Task
from assemblyline.odm.randomizer import random_minimal_obj
from assemblyline.common.constants import SERVICE_STATE_HASH
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline.common import forge
from assemblyline.odm import randomizer
from assemblyline.remote.datatypes import get_client

from assemblyline_service_server import app
from assemblyline_service_server.api.v1 import task
from assemblyline_service_server.config import AUTH_KEY


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


headers = {
    'Container-Id': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': service_name,
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'Timeout': 1,
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture(scope='function')
def storage():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.task.STORAGE', ds):
        yield ds


@pytest.fixture(scope='function')
def heuristics():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.task.heuristics', ds):
        yield ds


@pytest.fixture(scope='function')
def dispatch_client():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.task.dispatch_client', ds):
        yield ds


@pytest.fixture()
def client(redis, storage, heuristics, dispatch_client):
    client = app.app.test_client()
    task.status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60 * 30, host=redis)
    yield client


def test_task_timeout(client, dispatch_client):
    dispatch_client.request_work.return_value = None
    resp = client.get('/api/v1/task/', headers=headers)
    assert resp.status_code == 200
    assert not resp.json['api_response']['task']


def test_task_ignored_then_timeout(client, dispatch_client, storage):
    # Put a task "in the queue"
    task = random_minimal_obj(Task)
    task.ignore_cache = False
    dispatch_client.request_work.side_effect = [task, None]
    dispatch_client.schedule_builder.services[service_name].timeout = 100
    dispatch_client.schedule_builder.services[service_name].disable_cache = False

    resp = client.get('/api/v1/task/', headers=headers)
    assert resp.status_code == 200
    assert dispatch_client.service_finished.call_count == 1
    assert not resp.json['api_response']['task']


def test_task_dispatch(client, dispatch_client, storage):
    # Put a task "in the queue"
    task = random_minimal_obj(Task)
    task.ignore_cache = False
    storage.result.get_if_exists.return_value = None
    dispatch_client.request_work.return_value = task
    dispatch_client.schedule_builder.services[service_name].timeout = 100
    dispatch_client.schedule_builder.services[service_name].disable_cache = False

    resp = client.get('/api/v1/task/', headers=headers)
    assert resp.status_code == 200
    assert resp.json['api_response']['task'] == task.as_primitives()


def test_finish_error(client, dispatch_client):
    task = random_minimal_obj(Task)
    error = random_minimal_obj(Error)
    message = {'task': task.as_primitives(), 'error': error.as_primitives()}
    resp = client.post('/api/v1/task/', headers=headers, json=message)
    assert resp.status_code == 200
    assert dispatch_client.service_failed.call_count == 1
    assert dispatch_client.service_failed.call_args[0][0] == task.sid
    assert dispatch_client.service_failed.call_args[0][2] == error


def test_finish_minimal(client, dispatch_client):
    task = random_minimal_obj(Task)
    result = random_minimal_obj(Result)
    message = {'task': task.as_primitives(), 'result': result.as_primitives()}
    resp = client.post('/api/v1/task/', headers=headers, json=message)
    assert resp.status_code == 200
    assert dispatch_client.service_finished.call_count == 1
    assert dispatch_client.service_finished.call_args[0][0] == task.sid
    assert dispatch_client.service_finished.call_args[0][2] == result


# def test_finish_heuristic():
#     raise NotImplementedError()
#
#
# def test_finish_missing_file():
#     raise NotImplementedError()


# def test_flush_on_disable(tasking_namespace, ds, redis):
#     """TODO move this test and feature to dispatcher?"""
#     dc = tasking_namespace.dispatch_client.service_failed = mock.MagicMock()
#
#     work_queue = NamedQueue(service_queue_name(service_name), host=redis)
#     for ii in range(10):
#         body = b'abc' + str(ii).encode()
#         work_queue.push(ServiceTask({
#             'fileinfo': {
#                 'magic': 'file',
#                 'md5': hashlib.md5(body).hexdigest(),
#                 'mime': 'file',
#                 'sha1': hashlib.sha1(body).hexdigest(),
#                 'sha256': hashlib.sha256(body).hexdigest(),
#                 'size': 100,
#                 'type': 'unknown',
#             },
#             'service_name': service_name,
#             'max_files': 10,
#             'ttl': 10,
#         }).as_primitives())
#
#     ds.service_delta.update(service_name, [
#         (ds.service_delta.UPDATE_SET, 'enabled', False)
#     ])
#
#     start = time.time()
#     while time.time() - start < 120 and work_queue.length() != 0:
#         time.sleep(0.1)
#
#     assert dc.call_count >= 10
#
#     ds.service_delta.update(service_name, [
#         (ds.service_delta.UPDATE_SET, 'enabled', True)
#     ])
#
#