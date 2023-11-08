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


@badlist_api.route("/ssdeep/", methods=["POST"])
@api_login()
def similar_ssdeep(**_):
    """
    Check if a file with a similar SSDeep exists.

    Variables:
    None

    Arguments:
    None

    Data Block:
    {
        ssdeep : value    => Hash to check
    }

    API call example:
    GET /api/v1/badlist/ssdeep/

    Result example:
    <Badlisting object>
    """
    ssdeep = request.json.get('ssdeep', None)
    if not ssdeep:
        return make_api_response([], "You must provide an ssdeep value", 400)

    badlist = BADLIST_CLIENT.find_similar_ssdeep(ssdeep)
    if badlist:
        return make_api_response(badlist)
    return make_api_response([], "The hash was not found in the badlist.", 404)


@badlist_api.route("/tlsh/", methods=["POST"])
@api_login()
def similar_tlsh(**_):
    """
    Check if a file with a similar TLSH exists.

    Variables:
    None

    Arguments:
    None

    Data Block:
    {
        tlsh : value    => Hash to check
    }

    API call example:
    GET /api/v1/badlist/tlsh/

    Result example:
    <Badlisting object>
    """
    tlsh = request.json.get('tlsh', None)
    if not tlsh:
        return make_api_response([], "You must provide a tlsh value", 400)

    badlist = BADLIST_CLIENT.find_similar_tlsh(tlsh)
    if badlist:
        return make_api_response(badlist)
    return make_api_response([], "The hash was not found in the badlist.", 404)


@badlist_api.route("/tags/", methods=["POST"])
@api_login()
def tags_exists(**_):
    """
    Check if the provided tags exists in the badlist

    Variables:
    None

    Arguments:
    None

    Data Block:
    { # Dictionary of types -> values to check if exists
        "network.dynamic.domain": [...],
        "network.static.ip": [...]
    }

    API call example:
    GET /api/v1/badlist/tags/

    Result example:
    [ # List of existing objecs
        <badlisting object>,
        <Badlisting object>
    ]
    """
    data = request.json
    return make_api_response(BADLIST_CLIENT.exists_tags(data))
