
import pytest

from unittest.mock import MagicMock, patch

from assemblyline.odm import randomizer
from assemblyline.odm.models.safelist import Safelist
from assemblyline_service_server import app
from assemblyline_service_server.config import AUTH_KEY

headers = {
    'Container-Id': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': 'Safelist',
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'Timeout': 1,
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture(scope='function')
def storage():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.safelist.STORAGE', ds):
        yield ds


@pytest.fixture()
def client():
    client = app.app.test_client()
    yield client


# noinspection PyUnusedLocal
def test_safelist_exist(client, storage):
    valid_hash = randomizer.get_random_hash(64)
    valid_resp = randomizer.random_model_obj(Safelist, as_json=True)
    valid_resp['hashes']['sha256'] = valid_hash
    storage.safelist.get_if_exists.return_value = valid_resp

    resp = client.get(f'/api/v1/safelist/{valid_hash}/', headers=headers)
    assert resp.status_code == 200
    assert resp.json['api_response'] == valid_resp


# noinspection PyUnusedLocal
def test_safelist_missing(client, storage):
    invalid_hash = randomizer.get_random_hash(64)
    storage.safelist.get_if_exists.return_value = None

    resp = client.get(f'/api/v1/safelist/{invalid_hash}/', headers=headers)
    assert resp.status_code == 404
    assert resp.json['api_response'] is None


# noinspection PyUnusedLocal
def test_get_full_safelist(client, storage):
    storage.safelist.search = {
        "offset": 0,
        "rows": 0,
        "total": 0,
        "items": []
    }

    resp = client.get('/api/v1/safelist/', headers=headers)
    assert resp.status_code == 200
    assert 'match' in resp.json['api_response']
    assert 'regex' in resp.json['api_response']
    assert isinstance(resp.json['api_response']['match'], dict)
    assert isinstance(resp.json['api_response']['regex'], dict)


# noinspection PyUnusedLocal
def test_get_full_safelist_specific(client, storage):
    storage.safelist.search = {
        "offset": 0,
        "rows": 0,
        "total": 0,
        "items": []
    }

    tag_type = "network.dynamic.domain"
    resp = client.get(f'/api/v1/safelist/?tags={tag_type}', headers=headers)
    assert resp.status_code == 200
    assert 'match' in resp.json['api_response']
    assert 'regex' in resp.json['api_response']
    assert isinstance(resp.json['api_response']['match'], dict)
    assert isinstance(resp.json['api_response']['regex'], dict)

    for k in resp.json['api_response']['match']:
        assert k == tag_type

    for k in resp.json['api_response']['regex']:
        assert k == tag_type


# noinspection PyUnusedLocal
def test_get_signature_safelist(client, storage):
    storage.safelist.search = {
        "offset": 0,
        "rows": 1,
        "total": 1,
        "items": [{"signature": {"name": "test"}}]
    }

    resp = client.get('/api/v1/safelist/', headers=headers)
    assert resp.status_code == 200
    assert isinstance(resp.json['api_response'], list)
    assert resp.json['api_response'] == ['test']
