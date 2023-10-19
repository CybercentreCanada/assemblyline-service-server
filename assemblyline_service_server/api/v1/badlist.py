from flask import request

from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
from assemblyline_service_server.config import BADLIST_CLIENT
from assemblyline_service_server.helper.response import make_api_response

SUB_API = 'badlist'
badlist_api = make_subapi_blueprint(SUB_API, api_version=1)
badlist_api._doc = "Query badlisted hashes"


@badlist_api.route("/<qhash>/", methods=["GET"])
@api_login()
def exists(qhash, **_):
    """
    Check if a file exists in the badlist.

    Variables:
    qhash       => Hash to check

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/badlist/123456...654321/

    Result example:
    <Badlisting object>
    """
    badlist = BADLIST_CLIENT.exists(qhash)
    if badlist:
        return make_api_response(badlist)
    return make_api_response(None, "The hash was not found in the badlist.", 404)


@badlist_api.route("/", methods=["GET"])
@api_login()
def get_badlisted_tags(**_):
    """
    Get all the badlisted tags in the system

    Variables:
    tags       =>  List of tag types (comma seperated)

    Arguments:
    None

    Data Block:
    None

    API call example:
    GET /api/v1/badlist/?tags=network.static.domain,network.dynamic.domain

    Result example:
    {  # List of direct matches by tag type
       "network.static.domain": ["domain.bad"],
       "network.dynamic.domain": ["updates.micros0ft.com"]
    }
    """
    tag_types = request.args.get('tag_types', None)
    return make_api_response(BADLIST_CLIENT.get_badlisted_tags(tag_types))
