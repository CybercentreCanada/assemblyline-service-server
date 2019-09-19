import hashlib
import json
from typing import cast, Dict, Any, Optional

from flask import request

from assemblyline.common import forge
from assemblyline.common.attack_map import attack_map
from assemblyline.common.forge import CachedObject
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, api_login
from assemblyline_service_server.config import LOGGER, STORAGE
from assemblyline_service_server.helper.heuristics import get_heuristics

config = forge.get_config()

DISPATCH_CLIENT = DispatchClient(STORAGE)
HEURISTICS = cast(Dict[str, Heuristic], CachedObject(get_heuristics, refresh=300))

SUB_API = 'task'
task_api = make_subapi_blueprint(SUB_API, api_version=1)
task_api._doc = "Perform operations on service tasks"


@task_api.route("/", methods=["GET"])
@api_login()
def get_task(client_info):
    """

    Header:
    {'container_id': abcd...123
     'service_name': 'Extract',
     'service_version': '4.0.1',
     'service_tool_version': '
     'timeout': '30'

    }

    Result example:
    {'keep_alive': true}

    """
    timeout = int(request.headers.get('timeout', 30))

    counter = MetricsFactory('service', Metrics, name=client_info['service_name'], config=config)

    task, first_issue = DISPATCH_CLIENT.request_work(client_info['service_name'], timeout=timeout)

    if not task:
        # No task found in service queue
        return make_api_response(dict(task=False))

    counter.increment('execute')

    conf_key = generate_conf_key(client_info['service_tool_version'], task.service_config)
    result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                       service_name=client_info['service_name'],
                                       service_version=client_info['service_version'],
                                       conf_key=conf_key)

    # If we are allowed, try to see if the result has been cached
    if not task.ignore_cache:
        result = STORAGE.result.get_if_exists(result_key)
        if result:
            DISPATCH_CLIENT.service_finished(task.sid, result_key, result)
            return make_api_response(dict(task=False))

        # No luck with the cache, lets dispatch the task to a client
        counter.increment('cache_miss')

    return make_api_response(dict(task=task.as_primitives()))


@task_api.route("/", methods=["POST"])
@api_login()
def task_finished(client_info):
    """
    Header:
    {'client_id': 'abcd...123',
    }


    Data Block:
    {'exec_time': 300,
     'task': {},
     'result': ''
    }
    """
    data = request.json
    exec_time = data.get('exec_time')

    counter = MetricsFactory('service', Metrics, name=client_info['service_name'], config=config)

    try:
        task = ServiceTask(data['task'])

        if 'result' in data:  # Task created a result
            result = data['result']
            missing_files = handle_task_result(exec_time, task, result, counter, client_info)
            if missing_files:
                return make_api_response(dict(success=False, missing_files=missing_files))

            return make_api_response(dict(success=True))

        elif 'error' in data:  # Task created an error
            error = data['error']
            handle_task_error(exec_time, task, error, counter, client_info)
            return make_api_response(dict(success=True))
        else:
            return make_api_response("", "No result or error provided by service.", 400)

    except ValueError as e:  # Catch errors when building Task or Result model
        return make_api_response("", str(e), 400)


def handle_task_result(exec_time: int, task: ServiceTask, result: Dict[str, Any], counter: MetricsFactory,
                       client_info: Dict[str, str]):
    # Add scores to the heuristics, if any section set a heuristic
    total_score = 0
    for section in result['result']['sections']:
        if section.get('heuristic'):
            heur_id = section['heuristic']['heur_id']
            attack_id = section['heuristic'].get('attack_id')

            if HEURISTICS.get(heur_id):
                # Assign a score for the heuristic from the datastore
                section['heuristic']['score'] = HEURISTICS[heur_id].score
                total_score += HEURISTICS[heur_id].score

                if attack_id:
                    # Verify that the attack_id is valid
                    if attack_id not in attack_map:
                        LOGGER.warning(f"{client_info['client_id']} - {client_info['service_name']} "
                                       f"service specified an invalid attack_id in its service result, ignoring it")
                        # Assign an attack_id from the datastore if it exists
                        section['heuristic']['attack_id'] = HEURISTICS[heur_id].attack_id or None
                else:
                    # Assign an attack_id from the datastore if it exists
                    section['heuristic']['attack_id'] = HEURISTICS[heur_id].attack_id or None

    # Update the total score of the result
    result['result']['score'] = total_score

    result = Result(result)

    with forge.get_filestore() as f_transport:
        missing_files = []
        for file in (result.response.extracted + result.response.supplementary):
            if not f_transport.exists(file.sha256):
                missing_files.append(file.sha256)
        if missing_files:
            return missing_files

    conf_key = generate_conf_key(result.response.service_tool_version, task.service_config)
    result_key = result.build_key(conf_key)
    DISPATCH_CLIENT.service_finished(task.sid, result_key, result)

    # Metrics
    if result.result.score > 0:
        counter.increment('scored')
    else:
        counter.increment('not_scored')

    LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} "
                f"successfully completed task (SID: {task.sid}){f' in {exec_time}ms' if exec_time else ''}")


def handle_task_error(exec_time: int, task: ServiceTask, error: Dict[str, Any], counter: MetricsFactory,
                      client_info: Dict[str, str]) -> None:
    LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} "
                f"failed to complete task (SID: {task.sid}){f' in {exec_time}ms' if exec_time else ''}")

    error = Error(error)

    conf_key = generate_conf_key(error.response.service_tool_version, task.service_config)
    error_key = error.build_key(conf_key)
    DISPATCH_CLIENT.service_failed(task.sid, error_key, error)

    # Metrics
    if error.response.status == 'FAIL_RECOVERABLE':
        counter.increment('fail_recoverable')
    else:
        counter.increment('fail_nonrecoverable')


def generate_conf_key(service_tool_version: Optional[str], service_config: Dict[str, Any]):
    if service_tool_version is not None:
        service_tool_version_hash = hashlib.md5((service_tool_version.encode('utf-8'))).hexdigest()
    else:
        service_tool_version_hash = ''

    task_config_hash = hashlib.md5((json.dumps(sorted(service_config)).encode('utf-8'))).hexdigest()
    conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
    return conf_key
