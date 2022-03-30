import time

from flask import request
from werkzeug.exceptions import BadRequest

from assemblyline_core.tasking_client import ServiceMissingException
from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
from assemblyline_service_server.config import TASKING_CLIENT
from assemblyline_service_server.helper.response import make_api_response
from assemblyline_service_server.helper.metrics import get_metrics_factory


SUB_API = 'task'
task_api = make_subapi_blueprint(SUB_API, api_version=1)
task_api._doc = "Perform operations on service tasks"


@task_api.route("/", methods=["GET"])
@api_login()
def get_task(client_info):
    """
    Header:
    {'container_id': abcd...123
     'service_name': 'Extract',
     'service_version': '4.0.1',
     'service_tool_version': '
     'timeout': '30'}

    Result example:
    {'keep_alive': true}
    """
    service_name = client_info['service_name']
    service_version = client_info['service_version']
    service_tool_version = client_info['service_tool_version']
    client_id = client_info['client_id']
    remaining_time = timeout = int(float(request.headers.get('timeout', 30)))
    metric_factory = get_metrics_factory(service_name)

    start_time = time.time()
    status_expiry = start_time + timeout

    while remaining_time > 0:
        try:
            task, retry = TASKING_CLIENT.get_task(
                client_id, service_name, service_version, service_tool_version, metric_factory,
                status_expiry=status_expiry, timeout=remaining_time)
        except ServiceMissingException as e:
            return make_api_response({}, str(e), 404)

        if task is not None:
            return make_api_response(dict(task=task))
        elif not retry:
            return make_api_response(dict(task=False))

        # Recalculating how much time we have left before we reach the timeout
        remaining_time = start_time + timeout - time.time()

    # We've been processing cache hit for the length of the timeout... bailing out!
    return make_api_response(dict(task=False))


@task_api.route("/", methods=["POST"])
@api_login()
def task_finished(client_info):
    """
    Header:
    {'container_id': abcd...123
     'service_name': 'Extract',
     'service_version': '4.0.1',
     'service_tool_version': '
    }


    Data Block:
    {
     "exec_time": 300,
     "task": <Original Task Dict>,
     "result": <AL Result Dict>,
     "freshen": true
    }
    """
    try:
        service_name = client_info['service_name']
        response = TASKING_CLIENT.task_finished(request.json, client_info['client_id'],
                                                service_name, get_metrics_factory(service_name))
        if response:
            return make_api_response(response)
        return make_api_response("", "No result or error provided by service.", 400)
    except ValueError as e:  # Catch errors when building Task or Result model
        return make_api_response("", e, 400)
    except BadRequest:
        return make_api_response("", "Data received not in JSON format", 400)
