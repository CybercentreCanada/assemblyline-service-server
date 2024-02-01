import os
import shutil
import tempfile
from assemblyline_core.tasking_client import TaskingClientException

from flask import request

from assemblyline.filestore import FileStoreException
from assemblyline_service_server.helper.response import make_api_response, stream_file_response
from assemblyline_service_server.api.base import make_subapi_blueprint, api_login
from assemblyline_service_server.config import FILESTORE, LOGGER, TASKING_CLIENT

SUB_API = 'file'
file_api = make_subapi_blueprint(SUB_API, api_version=1)
file_api._doc = "Perform operations on file"


@file_api.route("/<sha256>/", methods=["GET"])
@api_login()
def download_file(sha256, client_info):
    """
    Download a file.

    Variables:
    sha256       => A resource locator for the file (sha256)

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/file/123456...654321/

    Result example:
    <THE FILE BINARY>
    """
    with tempfile.NamedTemporaryFile() as temp_file:
        try:
            FILESTORE.download(sha256, temp_file.name)
            f_size = os.path.getsize(temp_file.name)
            return stream_file_response(open(temp_file.name, 'rb'), sha256, f_size)
        except FileStoreException:
            LOGGER.exception(f"[{client_info['client_id']}] {client_info['service_name']} couldn't find file "
                             f"{sha256} requested by service ")
            return make_api_response({}, "The file was not found in the system.", 404)


@file_api.route("/", methods=["PUT"])
@api_login()
def upload_file(client_info):
    """
    Upload a single file.

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Files:
    Multipart file obj stored in the "file" key.

    API call example:
    PUT /api/v1/file/

    Result example:
    {"success": true}
    """
    sha256 = request.headers['sha256']
    classification = request.headers['classification']
    ttl = int(request.headers['ttl'])
    is_section_image = request.headers.get('is_section_image', 'false').lower() == 'true'
    is_supplementary = request.headers.get('is_supplementary', 'false').lower() == 'true'

    with tempfile.NamedTemporaryFile(mode='bw') as temp_file:
        # Try reading multipart data from 'files' or a single file post from stream
        if request.content_type.startswith('multipart'):
            file = request.files['file']
            file.save(temp_file.name)
        elif request.stream.is_exhausted:
            if request.stream.limit == len(request.data):
                temp_file.write(request.data)
            else:
                raise ValueError("Cannot find the uploaded file...")
        else:
            shutil.copyfileobj(request.stream, temp_file)

        try:
            TASKING_CLIENT.upload_file(temp_file.name, classification, ttl, is_section_image,
                                       is_supplementary, expected_sha256=sha256)
        except TaskingClientException as e:
            LOGGER.warning(f"{client_info['client_id']} - {client_info['service_name']}: {str(e)}")
            return make_api_response(dict(success=False), err=str(e), status_code=400)

    LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']}: "
                f"Successfully uploaded file (SHA256: {sha256})")

    return make_api_response(dict(success=True))
