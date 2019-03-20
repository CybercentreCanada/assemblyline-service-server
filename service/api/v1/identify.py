from flask import request
import logging
import os
import shutil

from service.api.base import make_api_response, make_subapi_blueprint
from assemblyline.common import identify

SUB_API = 'identify'
identify_api = make_subapi_blueprint(SUB_API, api_version=1)
identify_api._doc = "Identify"


@identify_api.route("/fileinfo/", methods=["POST"])
def fileinfo(**_):
    """
    Log an INFO message

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

    # out_dir = os.path.join(TEMP_SUBMIT_DIR, uuid4().get_hex())
    data = request.json
    if not data:
        return make_api_response({}, "Missing data block", 400)

    # name = data.get("name", None)
    # if not name:
    #     return make_api_response({}, "Filename missing", 400)

    # name = os.path.basename(name)
    # if not name:
    #     return make_api_response({}, "Invalid filename", 400)

    # out_file = os.path.join(out_dir, name)

    # try:
    #     os.makedirs(out_dir)
    # except Exception:
    #     pass

    # if os.path.exists(out_file):
    #     return make_api_response({}, "File already exist!?", 400)

    # binary = data.get("binary", None)
    # if binary:
    #     with open(out_file, "wb") as my_file:
    #         my_file.write(base64.b64decode(binary))
    # else:
    #     return make_api_response({}, "Missing file to get file info.", 400)
    file = str(data['path'])
    fi = identify.fileinfo(file)
    del fi['hex']
    return make_api_response(fi)
