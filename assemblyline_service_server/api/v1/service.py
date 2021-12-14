from assemblyline_core.tasking.helper.response import make_api_response
from flask import request
from assemblyline_service_server.api.base import api_login, make_subapi_blueprint, client

SUB_API = 'service'
service_api = make_subapi_blueprint(SUB_API, api_version=1)
service_api._doc = "Perform operations on service"


@service_api.route("/register/", methods=["PUT", "POST"])
@api_login()
def register_service(client_info):
    """
    Data Block:
    {
    TODO: service manifest
    }

    Result example:
    {'keep_alive': true}
    """
    try:
        return make_api_response(client.service.register_service(client_info, request.json))
    except ValueError as e:
        return make_api_response("", err=e, status_code=400)
