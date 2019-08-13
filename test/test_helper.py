import random

import pytest
import socketio

from assemblyline.common import forge
from assemblyline.common.classification import Classification
from assemblyline.odm import randomizer
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.service import Service
from assemblyline.odm.random_data import create_users, wipe_users
from assemblyline.odm.randomizer import random_model_obj
from service.config import AUTH_KEY

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
    def callback_register_service_new(keep_alive):
        assert keep_alive is False

    def callback_register_service_existing(keep_alive):
        assert keep_alive is True

    try:
        service_data = random_model_obj(Service, as_json=True)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_new)
        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_existing)
    finally:
        sio.disconnect()


def test_get_classification_definition(sio, datastore):
    def callback_get_classification_definition(classification_definition):
        assert Classification(classification_definition)

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_classification_definition)
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
    def callback_save_heuristics_new(new):
        assert new is True

    def callback_save_heuristics_existing(new):
        assert new is False

    try:
        heuristics = [randomizer.random_model_obj(Heuristic, as_json=True) for _ in range(random.randint(1, 6))]
        sio.emit('save_heuristics', heuristics, namespace='/helper', callback=callback_save_heuristics_new)
        sio.emit('save_heuristics', heuristics, namespace='/helper', callback=callback_save_heuristics_existing)
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
