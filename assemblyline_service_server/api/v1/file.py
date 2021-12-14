from flask import request

from assemblyline.filestore import FileStoreException
from assemblyline_core.tasking import client
from assemblyline_core.tasking.config import LOGGER
from assemblyline_core.tasking.helper.response import make_api_response
from assemblyline_service_server.api.base import make_subapi_blueprint, api_login, client

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
    try:
        return client.file.download_file(sha256)
    except FileStoreException:
        LOGGER.exception(f"[{client_info['client_id']}] {client_info['service_name']} couldn't find file "
                         f"{sha256} requested by service ")
        return make_api_response({}, "The file was not found in the system.", 404)


@file_api.route("/", methods=["PUT"])
@api_login()
def upload_files(client_info):
    """
    Upload multiple files.

    Variables:
    None

    Arguments:
    None

    Data Block:
    {<file #1 sha256>: {'classification': 'U',
                        'ttl': 15
                    },
    <file #2 sha256>: {'classification': 'U',
                        'ttl': 15
                    }
    }

    Files:

    4-tuple ('filename', fileobj, 'content_type', custom_headers),
    where 'content-type' is a string defining the content type of the given file
    and custom_headers a dict-like object containing additional headers to add for the file.

    API call example:
    PUT /api/v1/file/

    Result example:
    {"success": true}
    """
    success, error = client.file.upload_files(client_info, request)
    if not success:
        return make_api_response(dict(success=False), err=error, status_code=400)
    return make_api_response(dict(success=True), status_code=200)
