import json
import os
import shutil
import sys
import tempfile
import traceback

from flask import request
from requests_toolbelt import MultipartEncoder
from werkzeug.datastructures import FileStorage

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.result import Result
from service.api.base import make_api_response, make_subapi_blueprint, stream_multipart_response

SUB_API = 'file'

file_api = make_subapi_blueprint(SUB_API)
file_api._doc = "File manager"

config = forge.get_config()
datastore = forge.get_datastore()
filestore = forge.get_filestore()


@file_api.route("/download/<sha256>/", methods=["GET"])
def download_file(sha256, **_):
    file = filestore.get(sha256)
    fields = {'file': (sha256, file, 'application/text')}
    m = MultipartEncoder(fields=fields)
    return stream_multipart_response(m)


@file_api.route("/save/", methods=["GET"])
def save_file(**_):

    temp_dir = None
    try:
        # Load the Task and Result
        task = Task(json.loads(request.files['task_json'].read()))
        result_json = json.loads(request.files['result_json'].read())
        result = Result(result_json)

        expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)

        new_files = result.response.extracted + result.response.supplementary
        if new_files:
            # Create temp dir for downloading the files
            temp_dir = os.path.join(tempfile.gettempdir(), 'al', task.sid, task.service_name)
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            # Download the extracted and supplementary files to temp dir
            for f in new_files:
                path = os.path.join(temp_dir, f.sha256)
                FileStorage(request.files[f.sha256]).save(path)

            for f in new_files:
                file_path = os.path.join(temp_dir, f.sha256)
                file_info = identify.fileinfo(file_path)
                file_info['classification'] = result.classification
                file_info['expiry_ts'] = expiry_ts
                datastore.save_or_freshen_file(f.sha256, file_info, file_info['expiry_ts'], file_info['classification'])

                if not filestore.exists(f.sha256):
                    file = os.path.join(temp_dir, f.sha256)
                    filestore.upload(file, f.sha256)

        msg = 'success'
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        msg = repr(traceback.format_exception(exc_type, exc_value,
                                              exc_traceback))
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir)

    return make_api_response(msg)
