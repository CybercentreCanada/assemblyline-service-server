import os
import json
import cgi

from flask import request
from requests_toolbelt import MultipartEncoder

from assemblyline.odm.messages.task import TaskMessage
from assemblyline.odm.models.result import Result
from assemblyline.odm.randomizer import random_model_obj
from service.api.base import make_api_response, make_subapi_blueprint, stream_file_response, stream_multipart_response

SUB_API = 'task'

task_api = make_subapi_blueprint(SUB_API)
task_api._doc = "Task manager"


@task_api.route("/done/", methods=["POST"])
def done_task(**_):
    """
    Return a task and the file

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    A parsed classification definition.
    """
    for filename, file in request.files.items():
        file.save(os.path.join('/home/ubuntu', request.files[filename].filename))

    return make_api_response(str(msg))


@task_api.route("/file/<sha256>/", methods=["GET"])
def download_file(sha256, **_):
    """
    Return a task and the file

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    A parsed classification definition.
    """
    temp_target_file = "/home/ubuntu/pdf-test.pdf"
    f_size = os.path.getsize(temp_target_file)
    return stream_file_response(open(temp_target_file, 'rb'), "pdf-test.pdf", f_size)


@task_api.route("/get/", methods=["GET"])
def get_task(**_):
    """
    Return a task object and the file if requested

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    A parsed task definition and the file if requested
    """
    data = request.json

    task = random_model_obj(TaskMessage).as_primitives()

    # TODO: Remove following manual task changes (for testing only)
    task['msg']['fileinfo']['sha256'] = 'f6edcd8a1b4f7cb85486d0c6777f9174eadbc4d1d0d9e5aeba7132f30b34bc3e'

    task_fileinfo = task['msg']['fileinfo']
    task_json = json.dumps(task)

    temp_target_file = "/home/ubuntu/pdf-test.pdf"

    m = MultipartEncoder(fields={'task_json': ("data.json", task_json, 'application/json'),
                                 'file': (task_fileinfo['sha256'], open(temp_target_file, 'rb'), task_fileinfo['mime'])
                                 })

    return stream_multipart_response(m)
