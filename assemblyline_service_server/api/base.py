import functools
from sys import exc_info
from traceback import format_tb

import elasticapm
from flask import current_app, Blueprint, jsonify, make_response, Response, request

from assemblyline.common.str_utils import safe_str
from assemblyline_service_server.config import BUILD_LOWER, BUILD_MASTER, BUILD_NO, LOGGER, AUTH_KEY, config
from assemblyline_service_server.logger import log_with_traceback

API_PREFIX = "/api"
api = Blueprint("api", __name__, url_prefix=API_PREFIX)


def make_subapi_blueprint(name, api_version=1):
    """ Create a flask Blueprint for a subapi in a standard way. """
    return Blueprint(f"apiv{api_version}.{name}", name, url_prefix='/'.join([API_PREFIX, f"v{api_version}", name]))


####################################
# API Helper func and decorators
# noinspection PyPep8Naming,PyClassHasNoInit
class api_login:
    def __call__(self, func):
        @functools.wraps(func)
        def base(*args, **kwargs):
            # Before anything else, check that the API key is set
            apikey = request.environ.get('HTTP_X_APIKEY', None)
            if AUTH_KEY != apikey:
                client_id = request.headers.get('container_id', 'Unknown Client')
                header_dump = '; '.join(f'{k}={v}' for k, v in request.headers.items())
                wsgi_dump = '; '.join(f'{k}={v}' for k, v in request.environ.items())
                LOGGER.warning(f'Client [{client_id}] provided wrong api key [{apikey}] '
                               f'headers: {header_dump}; wsgi: {wsgi_dump}')
                return make_api_response("", "Unauthorized access denied", 401)

            client_info = dict(
                client_id=request.headers['container_id'],
                service_name=request.headers['service_name'],
                service_version=request.headers['service_version'],
                service_tool_version=request.headers.get('service_tool_version'),
            )

            if config.core.metrics.apm_server.server_url is not None:
                elasticapm.set_user_context(username=client_info['service_name'])

            kwargs['client_info'] = client_info
            return func(*args, **kwargs)

        return base


def make_api_response(data, err="", status_code=200, cookies=None):
    if isinstance(err, Exception):
        trace = exc_info()[2]
        err = ''.join(['\n'] + format_tb(trace) + [f"{err.__class__.__name__}: {str(err)}\n"]).rstrip('\n')
        log_with_traceback(LOGGER, trace, "Exception", is_exception=True)

    resp = make_response(jsonify({"api_response": data,
                                  "api_error_message": err,
                                  "api_server_version": f"{BUILD_MASTER}.{BUILD_LOWER}.{BUILD_NO}",
                                  "api_status_code": status_code}),
                         status_code)

    if isinstance(cookies, dict):
        for k, v in cookies.items():
            resp.set_cookie(k, v)

    return resp


def make_file_response(data, name, size, status_code=200, content_type="application/octet-stream"):
    response = make_response(data, status_code)
    response.headers["Content-Type"] = content_type
    response.headers["Content-Length"] = size
    response.headers["Content-Disposition"] = f'attachment; filename="{safe_str(name)}"'
    return response


def stream_file_response(reader, name, size, status_code=200):
    chunk_size = 65535

    def generate():
        reader.seek(0)
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            yield data

    headers = {"Content-Type": 'application/octet-stream',
               "Content-Length": size,
               "Content-Disposition": f'attachment; filename="{safe_str(name)}"'}
    return Response(generate(), status=status_code, headers=headers)


def make_binary_response(data, size, status_code=200):
    response = make_response(data, status_code)
    response.headers["Content-Type"] = 'application/octet-stream'
    response.headers["Content-Length"] = size
    return response


def stream_binary_response(reader, status_code=200):
    chunk_size = 4096

    def generate():
        reader.seek(0)
        while True:
            data = reader.read(chunk_size)
            if not data:
                break
            yield data

    return Response(generate(), status=status_code, mimetype='application/octet-stream')


#####################################
# API list API (API inception)
# noinspection PyUnusedLocal
@api.route("/")
def api_version_list(**kwargs):
    """
    List all available API versions.
    
    Variables: 
    None
    
    Arguments: 
    None
    
    Data Block:
    None
    
    Result example:
    ["v1"]         #List of API versions available
    """
    api_list = []
    for rule in current_app.url_map.iter_rules():
        if rule.rule.startswith("/api/"):
            version = rule.rule[5:].split("/", 1)[0]
            if version not in api_list and version != '':
                # noinspection PyBroadException
                try:
                    int(version[1:])
                except Exception:
                    continue
                api_list.append(version)

    return make_api_response(api_list)
