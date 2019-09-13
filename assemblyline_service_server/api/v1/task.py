import hashlib
import json
from typing import cast, Dict

from flask import request

from assemblyline.common.attack_map import attack_map
from assemblyline.common.forge import CachedObject
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response
from assemblyline_service_server.config import LOGGER, STORAGE
from assemblyline_service_server.helper.heuristics import get_heuristics

DISPATCH_CLIENT = DispatchClient()
HEURISTICS = cast(Dict[str, Heuristic], CachedObject(get_heuristics, refresh=300))

SUB_API = 'task'
task_api = make_subapi_blueprint(SUB_API, api_version=1)
task_api._doc = "Perform operations on service tasks"


@task_api.route("/get/", methods=["GET"])
def get_task():
    """

    Data Block:
    {'service_name': 'Extract',
     'service_version': '4.0.1',
     'timeout': 30

    }

    Result example:
    {'keep_alive': true}

    """
    data = request.json
    service_name = data['service_name']
    timeout = data.get('timeout', 30)

    task, first_issue = DISPATCH_CLIENT.request_work(service_name, timeout=timeout)

    if not task:
        # No task found in service queue
        return make_api_response(dict(success=False))


@task_api.route("/success/", methods=["GET"])
def task_success():
    """

    Data Block:
    {'exec_time': 300,
     'task': {},
     'result': {}
     '

    }
    """
    data = request.json
    task = ServiceTask(data['task'])
    result = data['result']

    # Add scores to the heuristics, if any section set a heuristic
    total_score = 0
    for section in result['result']['sections']:
        if section.get('heuristic', None):
            heur_id = section['heuristic']['heur_id']
            attack_id = section['heuristic'].get('attack_id', None)

            if HEURISTICS.get(heur_id):
                # Assign a score for the heuristic from the datastore
                section['heuristic']['score'] = HEURISTICS[heur_id].score
                total_score += HEURISTICS[heur_id].score

                if attack_id:
                    # Verify that the attack_id is valid
                    if attack_id not in attack_map:
                        LOGGER.warning(f"SocketIO:{self.namespace} - {service_name} service specified "
                                       f"an invalid attack_id in its service result, ignoring it")
                        # Assign an attack_id from the datastore if it exists
                        section['heuristic']['attack_id'] = HEURISTICS[heur_id].attack_id or None
                else:
                    # Assign an attack_id from the datastore if it exists
                    section['heuristic']['attack_id'] = HEURISTICS[heur_id].attack_id or None

    # Update the total score of the result
    result['result']['score'] = total_score

    result = Result(result)
    if result.response.service_tool_version is not None:
        service_tool_version_hash = hashlib.md5((result.response.service_tool_version.encode('utf-8'))).hexdigest()
    else:
        service_tool_version_hash = ''
    task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
    conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
    result_key = result.build_key(conf_key)
    DISPATCH_CLIENT.service_finished(task.sid, result_key, result)
