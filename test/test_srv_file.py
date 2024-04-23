import hashlib
from unittest.mock import patch, MagicMock

import pytest

from assemblyline.common import forge
from assemblyline.odm import randomizer
from assemblyline_service_server.config import AUTH_KEY
from assemblyline_service_server import app


@pytest.fixture()
def client():
    client = app.app.test_client()
    yield client


headers = {
    'Container-ID': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': randomizer.get_random_service_name(),
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture(scope='function')
def file_datastore():
    ds = MagicMock()
    with patch('assemblyline_service_server.config.TASKING_CLIENT.datastore', ds):
        yield ds


def test_download_file(client, file_datastore):
    # Put the file in place
    fs = forge.get_filestore()
    file_size = 12345
    fs.put('test_file', b'x' * file_size)
    try:
        response = client.get('/api/v1/file/test_file/', headers=headers)
        assert response.status_code == 200
        assert response.data == (b'x' * file_size)
    finally:
        fs.delete('test_file')

    # Try getting it again where the datastore thinks its there but its missing from the filestore
    response = client.get('/api/v1/file/test_file/', headers=headers)
    assert response.status_code == 404

    # Have the datastore say it doesn't exist
    file_datastore.file.get.return_value = None
    response = client.get('/api/v1/file/test_file/', headers=headers)
    assert response.status_code == 404


def test_upload_new_file(client, file_datastore):
    fs = forge.get_filestore()

    file_size = 10003
    file_data = b'x'*file_size
    file_hash = hashlib.sha256(file_data).hexdigest()

    fs.delete(file_hash)

    file_headers = dict(headers)
    file_headers['sha256'] = file_hash
    file_headers['classification'] = 'U'
    file_headers['ttl'] = 1
    file_headers['Content-Type'] = 'application/octet-stream'

    try:
        response = client.put('/api/v1/file/', headers=file_headers, data=file_data)
        assert response.status_code == 200
        assert fs.exists(file_hash)
        assert file_datastore.save_or_freshen_file.call_count == 1
    finally:
        fs.delete(file_hash)

def test_upload_section_image(client, file_datastore):
    fs = forge.get_filestore()

    file_size = 10003
    file_data = b'x'*file_size
    file_hash = hashlib.sha256(file_data).hexdigest()

    fs.delete(file_hash)

    file_headers = dict(headers)
    file_headers['sha256'] = file_hash
    file_headers['classification'] = 'U'
    file_headers['ttl'] = 1
    file_headers['Content-Type'] = 'application/octet-stream'
    file_headers['Is-Section-Image'] = 'true'

    try:
        response = client.put('/api/v1/file/', headers=file_headers, data=file_data)
        assert response.status_code == 200
        assert fs.exists(file_hash)
        assert file_datastore.save_or_freshen_file.call_count == 1
        assert file_datastore.file.get("sha256").is_section_image
    finally:
        fs.delete(file_hash)


def test_upload_file_bad_hash(client, file_datastore):
    fs = forge.get_filestore()

    file_size = 10003
    file_data = b'x'*file_size
    file_hash = hashlib.sha256(file_data).hexdigest()
    bad_hash = '0000' + file_hash[4:]

    fs.delete(file_hash)
    fs.delete(bad_hash)

    file_headers = dict(headers)
    file_headers['sha256'] = bad_hash
    file_headers['classification'] = 'U'
    file_headers['ttl'] = 1
    file_headers['Content-Type'] = 'application/octet-stream'

    try:
        response = client.put('/api/v1/file/', headers=file_headers, data=file_data)
        assert response.status_code in range(400, 500)
        assert not fs.exists(file_hash)
        assert not fs.exists(bad_hash)
        assert file_datastore.save_or_freshen_file.call_count == 0
    finally:
        fs.delete(file_hash)
        fs.delete(bad_hash)
