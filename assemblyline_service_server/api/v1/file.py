import os
import tempfile

from flask import request

from assemblyline.common import forge, identify
from assemblyline.common.isotime import now_as_iso
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, stream_file_response, \
    api_login
from assemblyline_service_server.config import LOGGER, STORAGE

SUB_API = 'file'
file_api = make_subapi_blueprint(SUB_API, api_version=1)
file_api._doc = "Perform operations on file"


@file_api.route("/<sha256>/", methods=["GET"])
@api_login()
def download_file(sha256):
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
    file_obj = STORAGE.file.get(sha256, as_obj=False)

    if not file_obj:
        return make_api_response({}, "The file was not found in the system.", 404)

    with forge.get_filestore() as f_transport, tempfile.TemporaryFile() as temp_file:
        f_transport.download(sha256, temp_file.name)
        f_size = os.path.getsize(temp_file)

        if f_size == 0:  # TODO: is this the correct way to check if the filestore doesn't have the file?
            return make_api_response({}, "The file was not found in the system.", 404)

        return stream_file_response(open(temp_file, 'rb'), sha256, f_size)


@file_api.route("/", methods=["PUT"])
@api_login()
def upload_files():
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

    API call example:
    PUT /api/v1/file/

    Result example:
    {"success": true}
    """
    data = request.json

    with forge.get_filestore() as f_transport:
        for sha256, file_obj in request.files.items():
            with tempfile.NamedTemporaryFile() as temp_file:
                # Write the file contents to the temporary file
                temp_file.write(file_obj.stream.read())

                # Identify the file info of the uploaded file
                file_info = identify.fileinfo(temp_file.name)

                # Validate SHA256 of the uploaded file
                if sha256 != file_info['sha256']:
                    LOGGER.info(f"SHA256 of received file from {'service_name'} service client doesn't match: "
                                f"{sha256} != {file_info['sha256']}")
                    # TODO: handle situation when sha256 doesn't match, let the client know of all the failed files,
                    #       so that it can try again

                file_info['classification'] = data[sha256]['classification']
                file_info['expiry_ts'] = now_as_iso(data[sha256]['ttl'] * 24 * 60 * 60)

                # Update the datastore with the uploaded file
                STORAGE.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'],
                                             file_info['classification'])

                # Upload file to the filestore if it doesn't already exist
                if not f_transport.exists(file_info['sha256']):
                    f_transport.upload(temp_file.name, file_info['sha256'])

    return make_api_response(dict(success=True))
