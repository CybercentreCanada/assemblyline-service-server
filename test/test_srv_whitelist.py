
import pytest

from unittest.mock import MagicMock, patch

from assemblyline.odm import randomizer
from assemblyline.odm.models.whitelist import Whitelist
from assemblyline_service_server import app
from assemblyline_service_server.config import AUTH_KEY

headers = {
    'Container-Id': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': 'Whitelist',
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'Timeout': 1,
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture(scope='function')
def storage():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.whitelist.STORAGE', ds):
        yield ds


@pytest.fixture()
def client():
    client = app.app.test_client()
    yield client


# noinspection PyUnusedLocal
def test_whitelist_exist(client, storage):
    valid_hash = randomizer.get_random_hash(64)
    valid_resp = randomizer.random_model_obj(Whitelist, as_json=True)
    valid_resp['fileinfo']['sha256'] = valid_hash
    storage.whitelist.get_if_exists.return_value = valid_resp

    resp = client.get(f'/api/v1/whitelist/{valid_hash}/', headers=headers)
    assert resp.status_code == 200
    assert resp.json['api_response'] == valid_resp


# noinspection PyUnusedLocal
def test_whitelist_missing(client, storage):
    invalid_hash = randomizer.get_random_hash(64)
    storage.whitelist.get_if_exists.return_value = None

    resp = client.get(f'/api/v1/whitelist/{invalid_hash}/', headers=headers)
    assert resp.status_code == 404
    assert resp.json['api_response'] is None
