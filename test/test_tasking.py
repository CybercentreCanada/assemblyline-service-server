from unittest.mock import patch, MagicMock

import pytest

from assemblyline.odm.models.result import Result
from assemblyline.odm.models.error import Error
from assemblyline.odm.messages.task import Task
from assemblyline.odm.randomizer import random_minimal_obj, random_model_obj
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
    dispatch_client.service_data[service_name].timeout = 100
    dispatch_client.service_data[service_name].disable_cache = False

    resp = client.get('/api/v1/task/', headers=headers)
    assert resp.status_code == 200
    assert dispatch_client.service_finished.call_count == 1
    assert not resp.json['api_response']['task']


def test_task_dispatch(client, dispatch_client, storage):
    # Put a task "in the queue"
    task = random_minimal_obj(Task)
    task.ignore_cache = False
    storage.result.get_if_exists.return_value = None
    storage.emptyresult.get_if_exists.return_value = None
    dispatch_client.request_work.return_value = task
    dispatch_client.service_data[service_name].timeout = 100
    dispatch_client.service_data[service_name].disable_cache = False

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
    error.archive_ts = dispatch_client.service_failed.call_args[0][2].archive_ts
    error.expiry_ts = dispatch_client.service_failed.call_args[0][2].expiry_ts
    error.created = dispatch_client.service_failed.call_args[0][2].created
    assert dispatch_client.service_failed.call_args[0][2] == error


def test_finish_minimal(client, dispatch_client):
    task = random_minimal_obj(Task)
    result = random_minimal_obj(Result)
    message = {'task': task.as_primitives(), 'result': result.as_primitives()}
    resp = client.post('/api/v1/task/', headers=headers, json=message)
    assert resp.status_code == 200
    assert dispatch_client.service_finished.call_count == 1
    assert dispatch_client.service_finished.call_args[0][0] == task.sid
    result.archive_ts = dispatch_client.service_finished.call_args[0][2].archive_ts
    result.expiry_ts = dispatch_client.service_finished.call_args[0][2].expiry_ts
    result.created = dispatch_client.service_finished.call_args[0][2].created
    assert dispatch_client.service_finished.call_args[0][2] == result


def test_finish_heuristic(client, dispatch_client, heuristics):
    task = random_minimal_obj(Task)

    result: Result = random_model_obj(Result)
    while not any(sec.heuristic for sec in result.result.sections):
        result: Result = random_model_obj(Result)

    heuristics_count = sum(int(sec.heuristic is not None) for sec in result.result.sections)

    result.result.score = 99999
    result.response.extracted = []
    result.response.supplementary = []

    message = {'task': task.as_primitives(), 'result': result.as_primitives()}
    resp = client.post('/api/v1/task/', headers=headers, json=message)
    assert resp.status_code == 200
    assert dispatch_client.service_finished.call_count == 1
    assert dispatch_client.service_finished.call_args[0][0] == task.sid
    # Mock objects are always one on conversion to int, being changed to this, means that it looked at the
    # mocked out heuristics to load the score.
    assert dispatch_client.service_finished.call_args[0][2].result.score == 1
    assert heuristics.get.call_count == heuristics_count


def test_finish_missing_file(client, dispatch_client, heuristics):
    task = random_minimal_obj(Task)
    fs = forge.get_filestore()

    result: Result = random_minimal_obj(Result)
    while not result.response.extracted:
        result: Result = random_model_obj(Result)
        result.response.extracted = [x for x in result.response.extracted if not fs.exists(x.sha256)]
    missing = {x.sha256 for x in result.response.extracted if not fs.exists(x.sha256)}
    missing |= {x.sha256 for x in result.response.supplementary if not fs.exists(x.sha256)}

    message = {'task': task.as_primitives(), 'result': result.as_primitives()}
    resp = client.post('/api/v1/task/', headers=headers, json=message)
    assert resp.status_code == 200
    assert resp.json['api_response']['success'] is False
    assert set(resp.json['api_response']['missing_files']) == missing
