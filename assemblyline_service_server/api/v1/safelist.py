from flask import request

from assemblyline_core.tasking import client
from assemblyline_core.tasking.helper.response import make_api_response
from assemblyline_service_server.api.base import api_login, make_subapi_blueprint, client

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
    safelist = client.safelist.exists(qhash, _)
    if safelist:
        return make_api_response(safelist)
    return make_api_response(None, "The hash was not found in the safelist.", 404)


@safelist_api.route("/", methods=["GET"])
@api_login()
def get_safelist_for_tags(**_):
    """
    Get the safelist for a given list of tags

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
    tag_types = request.args.get('tags', None)
    return make_api_response(client.safelist.get_safelist_for_tags(tag_types, **_))


@safelist_api.route("/signatures/", methods=["GET"])
@api_login()
def get_safelist_for_signatures(**_):
    """
    Get the safelist for all heuristic's signatures

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
    return make_api_response(client.safelist.get_safelist_for_signatures(**_))
