from flask import request

from assemblyline_service_server.helper.response import make_api_response
from assemblyline_service_server.api.base import api_login, make_subapi_blueprint, client

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
    try:
        return make_api_response(client.get_task(client_info, request.headers))
    except KeyError:
        return make_api_response({}, "The service you're asking task for does not exist, try later", 404)


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
        response = client.task_finished(client_info, request.json)
        if response:
            return make_api_response(response)
        return make_api_response("", "No result or error provided by service.", 400)
    except ValueError as e:
        return make_api_response("", e, 400)
