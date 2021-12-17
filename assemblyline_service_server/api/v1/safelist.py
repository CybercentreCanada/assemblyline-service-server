from flask import request

from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
from assemblyline_service_server.config import SAFELIST_CLIENT
from assemblyline_service_server.helper.response import make_api_response

SUB_API = 'safelist'
safelist_api = make_subapi_blueprint(SUB_API, api_version=1)
safelist_api._doc = "Query safelisted hashes"


@safelist_api.route("/<qhash>/", methods=["GET"])
@api_login()
def exists(qhash, **_):
    """
    Check if a file exists in the safelist.

    Variables:
    qhash       => Hash to check

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/safelist/123456...654321/

    Result example:
    <Safelisting object>
    """
    safelist = SAFELIST_CLIENT.exists(qhash)
    if safelist:
        return make_api_response(safelist)
    return make_api_response(None, "The hash was not found in the safelist.", 404)


@safelist_api.route("/", methods=["GET"])
@api_login()
def get_safelisted_tags(**_):
    """
    Get all the safelisted tags in the system

    Variables:
    tags       =>  List of tag types (comma seperated)

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/safelist/?tags=network.static.domain,network.dynamic.domain

    Result example:
    {
        "match": {  # List of direct matches by tag type
            "network.static.domain": ["google.ca"],
            "network.dynamic.domain": ["updates.microsoft.com"]
        },
        "regex": {  # List of regular expressions by tag type
            "network.static.domain": ["*.cyber.gc.ca"],
            "network.dynamic.domain": ["*.cyber.gc.ca"]
        }
    }
    """
    tag_types = request.args.get('tag_types', None)
    return make_api_response(SAFELIST_CLIENT.get_safelisted_tags(tag_types))


@safelist_api.route("/signatures/", methods=["GET"])
@api_login()
def get_safelisted_signatures(**_):
    """
    Get all the signatures that were safelisted in the system.

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/safelist/signatures/

    Result example:
    ["McAfee.Eicar", "Avira.Eicar", ...]
    """
    return make_api_response(SAFELIST_CLIENT.get_safelisted_signatures())
