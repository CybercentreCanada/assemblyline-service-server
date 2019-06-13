import pytest
import socketio

from service.config import AUTH_KEY


@pytest.fixture(scope="function")
def sio():
    sio = socketio.Client()
    headers = {
        'Service-API-Auth-Key': AUTH_KEY
    }

    sio.connect('http://localhost:5003', namespaces=['/helper'], headers=headers)

    return sio


def test_get_classification_definition(sio):

    def callback_get_classification_definition(classification_definition):
        assert classification_definition

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_classification_definition)
    finally:
        sio.disconnect()


def test_get_system_constants(sio):

    def callback_get_system_constants(constants):
        assert constants

    try:
        sio.emit('get_classification_definition', namespace='/helper', callback=callback_get_system_constants)
    finally:
        sio.disconnect()
