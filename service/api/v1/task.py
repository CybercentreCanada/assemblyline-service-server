import os
import json
import tempfile
import hashlib
import shutil

from flask import request
from requests_toolbelt import MultipartEncoder

from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.file import File
from service.api.base import make_api_response, make_subapi_blueprint, stream_file_response, stream_multipart_response

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.remote.datatypes.queues.named import NamedQueue, select
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.common.isotime import now_as_iso
from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name

SUB_API = 'task'

task_api = make_subapi_blueprint(SUB_API)
task_api._doc = "Task manager"

datastore = forge.get_datastore()
filestore = forge.get_filestore()
config = forge.get_config()
dispatch_client = DispatchClient(datastore)


@task_api.route("/done/", methods=["POST"])
def done_task(**_):
    # Load the Task and Result
    task = Task(json.loads(request.files['task_json'].read()))
    result_json = json.loads(request.files['result_json'].read())
    result_json['expiry_ts'] = now_as_iso(task.ttl * 24 * 60 * 60)
    result = Result(result_json)

    expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
    result.expiry_ts = expiry_ts
    temp_dir = None
    try:
        new_files = result.response.extracted + result.response.supplementary
        if new_files:
            # Create temp dir for downloading the files
            temp_dir = os.path.join(tempfile.gettempdir(), 'al', task.sid, task.service_name)
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            # Download the extracted and supplementary files to temp dir
            for f in new_files:
                path = os.path.join(temp_dir, f.sha256)
                with open(path, 'wb') as file:
                    file.write(request.files[f.sha256].read())
                    file.close()

            for f in new_files:
                if not datastore.file.get(f.sha256):
                    file_path = os.path.join(temp_dir, f.sha256)
                    file_info = identify.fileinfo(file_path)
                    file_info['classification'] = result.classification
                    file_info['expiry_ts'] = expiry_ts
                    file = File(file_info)
                    datastore.file.save(f.sha256, file)
                if not filestore.exists(f.sha256):
                    file = os.path.join(temp_dir, f.sha256)
                    filestore.upload(file, f.sha256)

        service_tool_version_hash = hashlib.md5((result.response.service_tool_version.encode('utf-8'))).hexdigest()
        task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
        conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
        result_key = result.build_key(conf_key)

        result.expiry_ts = expiry_ts
        dispatch_client.service_finished(task.sid, result_key, result)
        msg = 'success'
    except Exception as e:
        msg = str(e)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir)

    return make_api_response(msg)


@task_api.route("/file/<sha256>/", methods=["GET"])
def download_file(sha256, **_):
    temp_dir = None
    try:
        temp_dir = os.path.join(tempfile.gettempdir(), 'al', sha256)
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        file_path = os.path.join(temp_dir, sha256)
        filestore.download(sha256, file_path)
        with open(file_path, 'rb') as file:
            return stream_file_response(file, sha256, 12)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir)


@task_api.route("/get/", methods=["GET"])
def get_task(**_):
    data = request.json
    service_name = data['service_name']
    service_version = data['service_version']
    file_required = data['file_required']
    service_tool_version = data['service_tool_version']

    queue = [NamedQueue(service_queue_name(service_name))]

    while True:
        message = select(*queue, timeout=1)
        if not message:
            continue  # No task in queue

        queue, msg = message
        task = ServiceTask(msg)

        service_tool_version_hash = hashlib.md5((service_tool_version.encode('utf-8'))).hexdigest()
        task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
        conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

        result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                           service_name=service_name,
                                           service_version=service_version,
                                           conf_key=conf_key)

        result = datastore.result.get(result_key)
        if not result:
            task_json = json.dumps(task.as_primitives())

            fields = {}
            fields['task_json'] = ("task.json", task_json, 'application/json')

            if file_required:
                file = filestore.get(task.fileinfo.sha256)  # TODO: instead of get file, do download file temporarily and then get path and then read file content 'open(file, 'rb')'
                fields['file'] = (task.fileinfo.sha256, file, task.fileinfo.mime)

            m = MultipartEncoder(fields=fields)
            dispatch_client.running_tasks.set(task.key(), task.as_primitives())
            return stream_multipart_response(m)
        else:
            dispatch_client.service_finished(task.sid, result_key, result)
