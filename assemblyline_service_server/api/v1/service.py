from flask import request
from werkzeug.exceptions import BadRequest

from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
from assemblyline_service_server.config import TASKING_CLIENT
from assemblyline_service_server.helper.response import make_api_response

SUB_API = 'service'
service_api = make_subapi_blueprint(SUB_API, api_version=1)
service_api._doc = "Perform operations on service"


@service_api.route("/register/", methods=["PUT", "POST"])
@api_login()
def register_service(client_info):
    """
    Data Block:
    < SERVICE MANIFEST >

    Result example:
    {
        'keep_alive': true,
        'new_heuristics': [],
        'service_config': < APPLIED SERVICE CONFIG >
    }
    """
    try:
        output = TASKING_CLIENT.register_service(request.json, log_prefix=f"{client_info['client_id']} - ")
        return make_api_response(output)
    except ValueError as e:
        return make_api_response("", err=e, status_code=400)
    except BadRequest:
        return make_api_response("", "Data received not in JSON format.", 400)
