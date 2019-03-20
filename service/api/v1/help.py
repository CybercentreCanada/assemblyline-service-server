
from assemblyline.common import forge
from service.api.base import make_api_response, make_subapi_blueprint
from service.config import STORAGE


SUB_API = 'help'
constants = forge.get_constants()
config = forge.get_config()

help_api = make_subapi_blueprint(SUB_API)
help_api._doc = "Provide information about the system configuration"


@help_api.route("/classification_definition/", methods=["GET"])
def get_classification_definition(**_):
    """
    Return the current system classification definition

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    A parsed classification definition. (This is more for internal use)
    """
    return make_api_response(forge.get_classification().__dict__['original_definition'])


@help_api.route("/configuration/", methods=["GET"])
def get_system_configuration(**_):
    """
    Return the current system configuration:
        * Max file size
        * Max number of embedded files
        * Extraction's max depth
        * and many others...

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    {
        "<CONFIGURATION_ITEM>": <CONFIGURATION_VALUE>
    }
    """

    def get_config_item(parent, cur_item):
        if "." in cur_item:
            key, remainder = cur_item.split(".", 1)
            return get_config_item(parent[key], remainder)
        else:
            return parent.get(cur_item, None)

    cat_map = {}
    stg_map = {}

    for srv in STORAGE.list_all_services(as_obj=False):
        name = srv.get('name', None)
        cat = srv.get('category', None)
        if cat and name:
            temp_cat = cat_map.get(cat, [])
            temp_cat.append(name)
            cat_map[cat] = temp_cat

        stg = srv.get('stage', None)
        if stg and name:
            temp_stg = stg_map.get(stg, [])
            temp_stg.append(name)
            stg_map[stg] = temp_stg

    shareable_config_items = [
        "core.ingester.max_extracted",
        "core.ingester.max_supplementary",
        "services.categories",
        "services.limits.max_extracted",
        "services.limits.max_supplementary",
        "services.stages",
        "services.system_category",
        "submissions.max.priority",
        "submissions.max.size",
        "submissions.ttl",
        "ui.allow_raw_downloads",
        "ui.audit",
        "ui.download_encoding",
        "ui.enforce_quota"
    ]

    out = {}
    for item in shareable_config_items:
        out[item] = get_config_item(config, item)

    out["services.categories"] = [[x, cat_map.get(x, [])] for x in out.get("services.categories", None)]
    out["services.stages"] = [[x, stg_map.get(x, [])] for x in out.get("services.stages", None)]

    return make_api_response(out)


@help_api.route("/constants/", methods=["GET"])
def get_systems_constants(**_):
    """
    Return the current system configuration constants which includes:
        * Service tag contexts
        * Service tag types
        * File summary tags

    Variables:
    None

    Arguments:
    None

    Data Block:
    None

    Result example:
    {
        "STANDARD_TAG_CONTEXTS": [],
        "STANDARD_TAG_TYPES": [],
        "FILE_SUMMARY": []
    }
    """
    out = {"FILE_SUMMARY": constants.FILE_SUMMARY,
           "RECOGNIZED_TAGS": constants.RECOGNIZED_TAGS,
           "RULE_PATH": constants.RULE_PATH,
           "STANDARD_TAG_CONTEXTS": constants.STANDARD_TAG_CONTEXTS,
           "STANDARD_TAG_TYPES": constants.STANDARD_TAG_TYPES
           }

    return make_api_response(out)
