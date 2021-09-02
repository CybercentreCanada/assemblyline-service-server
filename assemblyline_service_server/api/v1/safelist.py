import yaml

from flask import request

from assemblyline.common import forge
from assemblyline_service_server.api.base import api_login, make_api_response, make_subapi_blueprint
from assemblyline_service_server.config import STORAGE, config

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
    safelist = STORAGE.safelist.get_if_exists(qhash, as_obj=False)
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
    if tag_types:
        tag_types = tag_types.split(',')

    with forge.get_cachestore('system', config=config, datastore=STORAGE) as cache:
        tag_safelist_yml = cache.get('tag_safelist_yml')
        if tag_safelist_yml:
            tag_safelist_data = yaml.safe_load(tag_safelist_yml)
        else:
            tag_safelist_data = forge.get_tag_safelist_data()

    if tag_types:
        output = {
            'match': {k: v for k, v in tag_safelist_data.get('match', {}).items() if k in tag_types or tag_types == []},
            'regex': {k: v for k, v in tag_safelist_data.get('regex', {}).items() if k in tag_types or tag_types == []},
        }
        for tag in tag_types:
            for sl in STORAGE.safelist.stream_search(f"type:tag AND enabled:true AND tag.type:{tag}", as_obj=False):
                output['match'].setdefault(sl['tag']['type'], [])
                output['match'][sl['tag']['type']].append(sl['tag']['value'])

    else:
        output = tag_safelist_data
        for sl in STORAGE.safelist.stream_search("type:tag AND enabled:true", as_obj=False):
            output['match'].setdefault(sl['tag']['type'], [])
            output['match'][sl['tag']['type']].append(sl['tag']['value'])

    return make_api_response(output)


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
    output = [
        item['signature']['name']
        for item in STORAGE.safelist.stream_search(
            "type:signature AND enabled:true", fl="signature.name", as_obj=False)]

    return make_api_response(output)
