
import pytest

from unittest.mock import MagicMock, patch

from assemblyline.odm import randomizer
from assemblyline.odm.models.badlist import Badlist
from assemblyline_service_server import app
from assemblyline_service_server.config import AUTH_KEY

headers = {
    'Container-Id': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': 'Badlist',
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'Timeout': 1,
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture(scope='function')
def storage():
    ds = MagicMock()
    with patch('assemblyline_service_server.config.BADLIST_CLIENT.datastore', ds):
        yield ds


@pytest.fixture()
def client():
    client = app.app.test_client()
    yield client


# noinspection PyUnusedLocal
def test_badlist_exist(client, storage):
    valid_hash = randomizer.get_random_hash(64)
    valid_resp = randomizer.random_model_obj(Badlist, as_json=True)
    valid_resp['hashes']['sha256'] = valid_hash
    storage.badlist.get_if_exists.return_value = valid_resp

    resp = client.get(f'/api/v1/badlist/{valid_hash}/', headers=headers)
    assert resp.status_code == 200
    assert resp.json['api_response'] == valid_resp


# noinspection PyUnusedLocal
def test_badlist_missing(client, storage):
    invalid_hash = randomizer.get_random_hash(64)
    storage.badlist.get_if_exists.return_value = None

    resp = client.get(f'/api/v1/badlist/{invalid_hash}/', headers=headers)
    assert resp.status_code == 404
    assert resp.json['api_response'] is None


# noinspection PyUnusedLocal
def test_get_full_badlist(client, storage):
    storage.badlist.stream_search.return_value = []

    resp = client.get('/api/v1/badlist/', headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json['api_response'], dict)


# noinspection PyUnusedLocal
def test_get_full_badlist_specific(client, storage):
    storage.badlist.stream_search.return_value = []

    tag_type = "network.dynamic.domain"
    resp = client.get(f'/api/v1/badlist/?tag_types={tag_type}', headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json['api_response'], dict)

    for k in resp.json['api_response']:
        assert k == tag_type
