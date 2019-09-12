import tempfile

from assemblyline_service_server import app

import time
import os
import os.path
import hashlib
import threading
import pytest
import socketio
import random
import flask_socketio

from assemblyline.common import forge
from assemblyline.common.classification import Classification
from assemblyline.odm import randomizer
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.service import Service
from assemblyline.odm.random_data import create_users, wipe_users
from assemblyline.odm.randomizer import random_model_obj
from assemblyline_service_server.config import AUTH_KEY

ds = forge.get_datastore()


def purge_socket():
    wipe_users(ds)


@pytest.fixture(scope="module")
def datastore(request):
    create_users(ds)
    request.addfinalizer(purge_socket)
    return ds


@pytest.fixture(scope="function")
def sio():
    sio = socketio.Client()
    headers = {
        'Container-Id': randomizer.get_random_hash(12),
        'Service-API-Auth-Key': AUTH_KEY,
        'Service-Name': randomizer.get_random_service_name(),
        'Service-Version': randomizer.get_random_service_version(),
        'Service-Tool-Version': randomizer.get_random_hash(64),
        'Service-Timeout': str(300),
    }

    sio.connect('http://localhost:5003', namespaces=['/helper'], headers=headers)

    return sio


@pytest.fixture(scope="function")
def helper():
    headers = {
        'Container-Id': randomizer.get_random_hash(12),
        'Service-API-Auth-Key': AUTH_KEY,
        'Service-Name': randomizer.get_random_service_name(),
        'Service-Version': randomizer.get_random_service_version(),
        'Service-Tool-Version': randomizer.get_random_hash(64),
        'Service-Timeout': str(300),
        'X-Forwarded-For': '127.0.0.1',
    }
    client = flask_socketio.SocketIOTestClient(app.app, app.socketio, namespace='/helper', headers=headers)
    yield client
    client.disconnect('/helper')


def test_register_service(sio, datastore):
    # Without events to wait on, due to the async nature of socketio, we will
    # disconnect before the callbacks ever run, this event lets us assert that it is actually called
    new_call = threading.Event()
    existing_call = threading.Event()

    def callback_register_service_new(keep_alive):
        assert not keep_alive
        new_call.set()

    def callback_register_service_existing(keep_alive):
        assert keep_alive is True
        existing_call.set()

    try:
        service_data = random_model_obj(Service, as_json=True)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_new)
        assert new_call.wait(5)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_existing)
        # Returns boolean based on whether the corresponding 'set' has been called before the end of the timeout
        # Effectively this is 'assert the callback has been called within 5 seconds'. Even though we call the asserts
        # in this order, we don't actually know a priori which order they are run in, but we should be testing that
        # based on the value of the keep_alive argument.
        assert existing_call.wait(5)
    finally:
        sio.disconnect()


def test_register_service_inproc(helper, datastore):
    service_data = random_model_obj(Service, as_json=True)
    assert helper.emit('register_service', service_data, namespace='/helper', callback=True) is False
    assert helper.emit('register_service', service_data, namespace='/helper', callback=True) is True


def test_get_classification_definition(sio, datastore):

    definition_called = threading.Event()

    def callback_get_classification_definition(classification_definition):
        assert Classification(classification_definition)
        definition_called.set()

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_classification_definition)
        assert definition_called.wait(5)
    finally:
        sio.disconnect()


def test_get_classification_definition_inproc(helper, datastore):
    classification_definition = helper.emit('get_classification_definition', namespace='/helper', callback=True)
    assert Classification(classification_definition)


def test_save_heuristics(sio, datastore):
    new_call = threading.Event()
    existing_call = threading.Event()

    def callback_save_heuristics_new(new):
        assert new
        new_call.set()

    def callback_save_heuristics_existing(new):
        assert not new
        existing_call.set()

    try:
        heuristics = [randomizer.random_model_obj(Heuristic, as_json=True) for _ in range(random.randint(1, 6))]
        sio.emit('save_heuristics', heuristics, namespace='/helper', callback=callback_save_heuristics_new)
        assert new_call.wait(5)
        sio.emit('save_heuristics', heuristics, namespace='/helper', callback=callback_save_heuristics_existing)
        assert existing_call.wait(5)
    finally:
        sio.disconnect()


def test_save_heuristics_inproc(helper, datastore):
    heuristics = [randomizer.random_model_obj(Heuristic, as_json=True) for _ in range(random.randint(1, 6))]
    assert helper.emit('save_heuristics', heuristics, namespace='/helper', callback=True)
    assert helper.emit('save_heuristics', heuristics, namespace='/helper', callback=True) is False
    assert helper.emit('save_heuristics', 'garbage', namespace='/helper', callback=True) is False


def test_start_download_inproc(helper):
    fs = forge.get_filestore()
    file_size = int(64 * 1024 * 1.9)  # 1.9 times the chunk size, so we should get two chunks
    fs.put('test_file', 'x' * file_size)
    try:
        helper.emit('start_download', 'test_file', 'file_path', namespace='/helper', callback=True)

        bytes_found = 0
        chunks_read = 0
        start = time.time()
        finished = False
        while time.time() - start < 10 and not finished:
            messages = helper.get_received('/helper')
            if not messages:
                time.sleep(0.01)

            for message in messages:
                path, offset, chunk, last_chunk = message.pop('args')
                chunks_read += 1
                bytes_found += len(chunk)
                finished |= bytes_found == file_size
                assert path == 'file_path'
                assert last_chunk == (bytes_found == file_size)
        assert chunks_read == 2

    finally:
        fs.delete('test_file')


def test_file_exists_inproc(helper):
    fs = forge.get_filestore()
    fs.put('test_file', 'x'*10)
    try:
        sha, file_path, classification, ttl = \
            helper.emit('file_exists', 'test_file', 'file_path', 'classification', 1, callback=True, namespace='/helper')
        assert sha is None
        assert file_path is None
        assert classification is None
        assert ttl is None
    finally:
        fs.delete('test_file')


def test_file_not_exists_inproc(helper):
    fs = forge.get_filestore()
    fs.delete('test_file')
    dest_path = ''
    try:
        sha, file_path, classification, ttl = \
            helper.emit('file_exists', 'test_file', 'file_path', 'classification', 1, callback=True, namespace='/helper')
        assert sha == 'test_file'
        assert file_path == 'file_path'
        assert classification == 'classification'
        assert ttl == 1

        dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha)
        assert not os.path.exists(dest_path)
    finally:
        fs.delete('test_file')
        if os.path.exists(dest_path):
            os.unlink(dest_path)


def test_upload_file_inproc(helper):
    dest_path = ''
    expected_body = b'iiiiijjjjj'
    fs = forge.get_filestore()
    sha = hashlib.sha256(expected_body).hexdigest()
    fs.delete(sha)
    try:
        _, _, _, _ = helper.emit('file_exists', sha, './temp_file', 'U', 1, callback=True, namespace='/helper')
        helper.emit('upload_file_chunk', 0, b'iiiii', False, 'U', sha, 1, namespace='/helper')
        helper.emit('upload_file_chunk', 5, b'jjjjj', True, 'U', sha, 1, namespace='/helper')

        assert fs.exists(sha)
        assert fs.get(sha) == expected_body

        message = helper.get_received('/helper')[0]
        assert message['name'] == 'upload_success'
        assert message['args'][0] is True

        dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha)
        assert not os.path.exists(dest_path)
    finally:
        fs.delete(sha)
        if os.path.exists(dest_path):
            os.unlink(dest_path)


@pytest.mark.xfail  # This still fails. Remove this marking and change the test when function fixed
def test_upload_file_bad_hash_inproc(helper):
    """Upload a file where the client provided hash is wrong.

    The file shouldn't be accepted into the system with either hash.
    TODO should the client be told to retry upload?
    """
    dest_path = None
    expected_body = b'xxxxxyyyyy'
    fs = forge.get_filestore()
    real_sha = hashlib.sha256(expected_body).hexdigest()
    sha = real_sha[:-4] + '0000'
    fs.delete(sha)
    try:
        _, _, _, _ = helper.emit('file_exists', sha, './temp_file', 'U', 1,
                                            callback=True, namespace='/helper')
        helper.emit('upload_file_chunk', 0, b'xxxxx', False, 'U', sha, 1, namespace='/helper')
        helper.emit('upload_file_chunk', 5, b'yyyyy', True, 'U', sha, 1, namespace='/helper')

        assert not fs.exists(sha)
        assert not fs.exists(real_sha)

        dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha)
        assert not os.path.exists(dest_path)
    finally:
        fs.delete(sha)
        fs.delete(real_sha)
        if dest_path and os.path.exists(dest_path):
            os.unlink(dest_path)