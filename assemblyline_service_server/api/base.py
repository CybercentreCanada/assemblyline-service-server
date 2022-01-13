import functools

import elasticapm
from flask import Blueprint, current_app, request

from assemblyline_service_server.config import AUTH_KEY, LOGGER, config
from assemblyline_service_server.helper.response import make_api_response

API_PREFIX = "/api"
api = Blueprint("api", __name__, url_prefix=API_PREFIX)


def make_subapi_blueprint(name, api_version=1):
    """ Create a flask Blueprint for a subapi in a standard way. """
    return Blueprint(name, name, url_prefix='/'.join([API_PREFIX, f"v{api_version}", name]))


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
