import pytest
import socketio

from assemblyline.common import forge
from assemblyline.odm import randomizer
from assemblyline.odm.random_data import create_users, wipe_users
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


def test_get_classification_definition(datastore, sio):
    def callback_get_classification_definition(classification_definition):
        assert classification_definition

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_classification_definition)
    finally:
        sio.disconnect()


def test_get_system_constants(datastore, sio):
    def callback_get_system_constants(constants):
        assert len(constants) == 5

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_system_constants)
    finally:
        sio.disconnect()


def test_register_service(datastore, sio):
    def callback_register_service_new(keep_alive):
        assert keep_alive is False

    def callback_register_service_existing(keep_alive):
        assert keep_alive is True

    try:
        service_data = {
            'name': 'TestService',
            'enabled': True,
            'category': 'Static Analysis',
            'stage': 'CORE',
            'version': '4.0.0'
        }

        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_new)

        sio.emit('register_service', service_data, namespace='/helper', callback=callback_register_service_existing)
    finally:
        sio.disconnect()
