from flask import request
import logging
import sys

from service.api.base import make_api_response, make_subapi_blueprint

SUB_API = 'log'
log_api = make_subapi_blueprint(SUB_API, api_version=1)
log_api._doc = "Log messages"

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


@log_api.route("/debug/", methods=["POST"])
def debug(**_):
    """
    Log DEBUG message

    Variables:
    None

    Arguments:
    None

    Data Block:
    {'log': 'assemblyline.svc.extract',
     'msg': 'debug message'}

    Result example:
    {"success": true }    # Debug message logged successfully
    """
    data = request.json

    try:
        log = logging.getLogger(data['log'])
        log.debug(data['msg'])
    except:
        return make_api_response("", "Could not log debug message", 400)

    return make_api_response({"success": True})


@log_api.route("/error/", methods=["POST"])
def error(**_):
    """
    Log ERROR message

    Variables:
    None

    Arguments:
    None

    Data Block:
    {'log': 'assemblyline.svc.extract',
     'msg': 'error message'}

    Result example:
    {"success": true }    # Error message logged successfully
    """
    data = request.json

    try:
        log = logging.getLogger(data['log'])
        log.error(data['msg'])
    except:
        return make_api_response("", "Could not log error message", 400)

    return make_api_response({"success": True})


@log_api.route("/info/", methods=["POST"])
def info(**_):
    """
    Log INFO message

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

    try:
        log = logging.getLogger(data['log'])
        log.info(data['msg'])
    except:
        return make_api_response("", "Could not log info message", 400)

    return make_api_response({"success": True})


@log_api.route("/warning/", methods=["POST"])
def warning(**_):
    """
    Log WARNING message

    Variables:
    None

    Arguments:
    None

    Data Block:
    {'log': 'assemblyline.svc.extract',
     'msg': 'warning message'}

    Result example:
    {"success": true }    # Warning message logged successfully
    """
    data = request.json

    try:
        log = logging.getLogger(data['log'])
        log.warning(data['msg'])
    except:
        return make_api_response("", "Could not log warning message", 400)

    return make_api_response({"success": True})
