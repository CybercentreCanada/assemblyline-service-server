import os
import shutil
import tempfile

from flask import request

from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline.filestore import FileStoreException
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, stream_file_response, \
    api_login
from assemblyline_service_server.config import FILESTORE, LOGGER, STORAGE, config

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
    sha256 = request.headers['sha256']
    classification = request.headers['classification']
    ttl = int(request.headers['ttl'])
    is_section_image = bool(request.headers.get('is_section_image', False))

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

        # Identify the file info of the uploaded file
        file_info = identify.fileinfo(temp_file.name)

        # Validate SHA256 of the uploaded file
        if sha256 == file_info['sha256']:
            file_info['archive_ts'] = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
            file_info['classification'] = classification
            if ttl:
                file_info['expiry_ts'] = now_as_iso(ttl * 24 * 60 * 60)
            else:
                file_info['expiry_ts'] = None

            # Update the datastore with the uploaded file
            STORAGE.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'],
                                         file_info['classification'], is_section_image=is_section_image)

            # Upload file to the filestore if it doesn't already exist
            if not FILESTORE.exists(file_info['sha256']):
                FILESTORE.upload(temp_file.name, file_info['sha256'])
        else:
            LOGGER.warning(f"{client_info['client_id']} - {client_info['service_name']} "
                           f"uploaded file (SHA256: {file_info['sha256']}) doesn't match "
                           f"expected file (SHA256: {sha256})")
            return make_api_response(dict(success=False),
                                     err=f"Uploaded file does not match expected "
                                         f"file hash. [{file_info['sha256']} != {sha256}]",
                                     status_code=400)

    LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} "
                f"successfully uploaded file (SHA256: {file_info['sha256']})")

    return make_api_response(dict(success=True))
