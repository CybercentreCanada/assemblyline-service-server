import threading
import pytest
import socketio
import random

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


def test_register_service(sio, datastore):
    # Without events to wait on, due to the async nature of socketio, we will
    # disconnect before the callbacks ever run, this event lets us assert that it is actually called
    new_call = threading.Event()
    existing_call = threading.Event()

    def callback_register_service_new(keep_alive):
        assert not keep_alive
        new_call.set()

    def callback_register_service_existing(keep_alive):
        assert keep_alive
        existing_call.set()

    try:
        service_data = random_model_obj(Service, as_json=True)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_new)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_existing)
        # Returns boolean based on whether the corresponding 'set' has been called before the end of the timeout
        # Effectively this is 'assert the callback has been called within 5 seconds'. Even though we call the asserts
        # in this order, we don't actually know a priori which order they are run in, but we should be testing that
        # based on the value of the keep_alive argument.
        assert new_call.wait(5)
        assert existing_call.wait(5)
    finally:
        sio.disconnect()


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


# def test_get_system_constants(sio):
#     def callback_get_system_constants(constants):
#         assert len(constants) == 5
#
#     try:
#         sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_system_constants)
#     finally:
#         sio.disconnect()


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
        sio.emit('save_heuristics', heuristics, namespace='/helper', callback=callback_save_heuristics_existing)
        assert new_call.wait(5)
        assert existing_call.wait(5)
    finally:
        sio.disconnect()


# def test_start_download(sio, datastore):
#     # TODO
#     pass
#
#
# def test_upload_file(sio, datastore):
#     # TODO
#     pass
