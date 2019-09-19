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
    'Container-Id': randomizer.get_random_hash(12),
    'X-APIKey': AUTH_KEY,
    'Service-Name': randomizer.get_random_service_name(),
    'Service-Version': randomizer.get_random_service_version(),
    'Service-Tool-Version': randomizer.get_random_hash(64),
    'X-Forwarded-For': '127.0.0.1',
}


@pytest.fixture()
def file_datastore():
    ds = MagicMock()
    with patch('assemblyline_service_server.api.v1.file.STORAGE', ds):
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



# def test_upload_file_inproc(helper):
#     dest_path = ''
#     expected_body = b'iiiiijjjjj'
#     fs = forge.get_filestore()
#     sha = hashlib.sha256(expected_body).hexdigest()
#     fs.delete(sha)
#     try:
#         _, _, _, _ = helper.emit('file_exists', sha, './temp_file', 'U', 1, callback=True, namespace='/helper')
#         helper.emit('upload_file_chunk', 0, b'iiiii', False, 'U', sha, 1, namespace='/helper')
#         message = helper.get_received('/helper')[0]
#         assert message['name'] == 'chunk_upload_success'
#         assert message['args'][0] is True
#
#         helper.emit('upload_file_chunk', 5, b'jjjjj', True, 'U', sha, 1, namespace='/helper')
#         assert message['name'] == 'chunk_upload_success'
#         assert message['args'][0] is True
#
#         assert fs.exists(sha)
#         assert fs.get(sha) == expected_body
#
#         dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha)
#         assert not os.path.exists(dest_path)
#     finally:
#         fs.delete(sha)
#         if os.path.exists(dest_path):
#             os.unlink(dest_path)
#
#
# @pytest.mark.xfail
# def test_upload_file_bad_hash_inproc(helper):
#     """Upload a file where the client provided hash is wrong.
#
#     The file shouldn't be accepted into the system with either hash.
#     TODO should the client be told to retry upload?
#     """
#     dest_path = None
#     expected_body = b'mmmmmnnnnn'
#     fs = forge.get_filestore()
#     real_sha = hashlib.sha256(expected_body).hexdigest()
#     sha = real_sha[:-4] + '0000'
#     fs.delete(sha)
#     try:
#         _, _, _, _ = helper.emit('file_exists', sha, './temp_file', 'U', 1, callback=True, namespace='/helper')
#         helper.emit('upload_file_chunk', 0, b'mmmmm', False, 'U', sha, 1, namespace='/helper')
#         helper.emit('upload_file_chunk', 5, b'nnnnn', True, 'U', sha, 1, namespace='/helper')
#
#         assert not fs.exists(sha)
#         assert not fs.exists(real_sha)
#
#         dest_path = os.path.join(tempfile.gettempdir(), 'uploads', sha)
#         assert not os.path.exists(dest_path)
#     finally:
#         fs.delete(sha)
#         fs.delete(real_sha)
#         if dest_path and os.path.exists(dest_path):
#             os.unlink(dest_path)