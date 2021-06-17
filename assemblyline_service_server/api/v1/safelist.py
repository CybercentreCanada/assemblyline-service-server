
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, api_login
from assemblyline_service_server.config import STORAGE

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
