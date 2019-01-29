from flask import request
import logging

from service.api.base import make_api_response, make_subapi_blueprint
from assemblyline.common.identify import fileinfo

SUB_API = 'identify'
identify_api = make_subapi_blueprint(SUB_API, api_version=1)
identify_api._doc = "Identify"


@identify_api.route("/fileinfo/", methods=["GET"])
def fileinfo(**_):
    """
    Log an INFO message

    Variables:
    None

    Arguments:
    None

    Data Block:
    {'log': 'assemblyline.svc.extract',
     'msg': 'info message'}

    Result example:
    {"success": true }    # Info message logged successfully
    """
    data = request.json

    path = data['path']

    return make_api_response(fileinfo(path))
