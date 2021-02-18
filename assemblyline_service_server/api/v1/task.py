import time
from typing import cast, Dict, Any

from assemblyline.common.dict_utils import flatten, unflatten
from assemblyline.common.heuristics import service_heuristic_to_result_heuristic, InvalidHeuristicException

from assemblyline.common.isotime import now_as_iso
from flask import request

from assemblyline.common import forge
from assemblyline.common.constants import SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common.forge import CachedObject
from assemblyline.odm import construct_safe
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.tagging import Tagging
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_service_server.api.base import make_subapi_blueprint, make_api_response, api_login
from assemblyline_service_server.config import LOGGER, STORAGE, config
from assemblyline_service_server.helper.heuristics import get_heuristics

status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60*30)
dispatch_client = DispatchClient(STORAGE)
heuristics = cast(Dict[str, Heuristic], CachedObject(get_heuristics, refresh=300))
tag_whitelister = forge.get_tag_whitelister(log=LOGGER)

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
    service_name = client_info['service_name']
    service_version = client_info['service_version']
    client_id = client_info['client_id']
    timeout = int(float(request.headers.get('timeout', 30)))
    # Add a little extra to the status timeout so that the service has a chance to retry before we start to
    # suspect it of slacking off
    status_table.set(client_id, (service_name, ServiceStatus.Idle, time.time() + timeout + 5))

    stats = {
        "execute": 1,
        "cache_miss": 0,
        "cache_hit": 0,
        "scored": 0,
        "not_scored": 0
    }

    task = dispatch_client.request_work(client_id, service_name, service_version, timeout=timeout)

    if not task:
        # No task found in service queue
        return make_api_response(dict(task=False))

    try:
        result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                           service_name=service_name,
                                           service_version=service_version,
                                           service_tool_version=client_info['service_tool_version'],
                                           is_empty=False,
                                           task=task)
        service_data = dispatch_client.service_data[service_name]

        # If we are allowed, try to see if the result has been cached
        if not task.ignore_cache and not service_data.disable_cache:
            result = STORAGE.result.get_if_exists(result_key)
            if result:
                stats['cache_hit'] += 1
                if result.result.score:
                    stats['scored'] += 1
                else:
                    stats['not_scored'] += 1
                dispatch_client.service_finished(task.sid, result_key, result)
                return make_api_response(dict(task=False))

            result = STORAGE.emptyresult.get_if_exists(f"{result_key}.e")
            if result:
                stats['cache_hit'] += 1
                stats['not_scored'] += 1
                result = STORAGE.create_empty_result_from_key(result_key)
                dispatch_client.service_finished(task.sid, f"{result_key}.e", result)
                return make_api_response(dict(task=False))

        # No luck with the cache, lets dispatch the task to a client
        stats['cache_miss'] += 1
        status_table.set(client_id, (service_name, ServiceStatus.Running, time.time() + service_data.timeout))
        return make_api_response(dict(task=task.as_primitives()))
    finally:
        export_metrics_once(service_name, Metrics, stats, host=client_id, counter_type='service')


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

    try:
        task = ServiceTask(data['task'])

        if 'result' in data:  # Task created a result
            missing_files = handle_task_result(exec_time, task, data['result'], client_info, data['freshen'])
            if missing_files:
                return make_api_response(dict(success=False, missing_files=missing_files))
            return make_api_response(dict(success=True))

        elif 'error' in data:  # Task created an error
            error = data['error']
            handle_task_error(exec_time, task, error, client_info)
            return make_api_response(dict(success=True))
        else:
            return make_api_response("", "No result or error provided by service.", 400)

    except ValueError as e:  # Catch errors when building Task or Result model
        return make_api_response("", e, 400)


def handle_task_result(exec_time: int, task: ServiceTask, result: Dict[str, Any], client_info: Dict[str, str],
                       freshen: bool):
    service_name = client_info['service_name']
    client_id = client_info['client_id']

    # Add scores to the heuristics, if any section set a heuristic
    total_score = 0
    for section in result['result']['sections']:
        if section.get('heuristic'):
            heur_id = f"{client_info['service_name'].upper()}.{str(section['heuristic']['heur_id'])}"
            section['heuristic']['heur_id'] = heur_id
            try:
                section['heuristic'] = service_heuristic_to_result_heuristic(section['heuristic'], heuristics)
                total_score += section['heuristic']['score']
            except InvalidHeuristicException:
                section['heuristic'] = None

    # Update the total score of the result
    result['result']['score'] = total_score

    # Add timestamps for creation, archive and expiry
    result['created'] = now_as_iso()
    result['archive_ts'] = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
    if task.ttl:
        result['expiry_ts'] = now_as_iso(task.ttl * 24 * 60 * 60)

    # Pop the temporary submission data
    temp_submission_data = result.pop('temp_submission_data', None)

    # Process the tag values
    for section in result['result']['sections']:
        # Perform tag whitelisting
        section['tags'] = unflatten(tag_whitelister.get_validated_tag_map(flatten(section['tags'])))

        section['tags'], dropped = construct_safe(Tagging, section.get('tags', {}))

        if dropped:
            LOGGER.warning(f"[{task.sid}] Invalid tag data from {client_info['service_name']}: {dropped}")

    result = Result(result)

    with forge.get_filestore() as f_transport:
        missing_files = []
        for file in (result.response.extracted + result.response.supplementary):
            cur_file_info = STORAGE.file.get_if_exists(file.sha256, as_obj=False)
            if cur_file_info is None or not f_transport.exists(file.sha256):
                missing_files.append(file.sha256)
            elif cur_file_info is not None and freshen:
                cur_file_info['archive_ts'] = result.archive_ts
                if task.ttl:
                    cur_file_info['expiry_ts'] = result.expiry_ts
                cur_file_info['classification'] = file.classification.value
                STORAGE.save_or_freshen_file(file.sha256, cur_file_info,
                                             cur_file_info['expiry_ts'], cur_file_info['classification'])
        if missing_files:
            return missing_files

    result_key = result.build_key(service_tool_version=result.response.service_tool_version, task=task)
    dispatch_client.service_finished(task.sid, result_key, result, temp_submission_data)

    # Metrics

    if result.result.score > 0:
        export_metrics_once(service_name, Metrics, dict(scored=1), host=client_id, counter_type='service')
    else:
        export_metrics_once(service_name, Metrics, dict(not_scored=1), host=client_id, counter_type='service')

    LOGGER.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
                f"successfully completed task {f' in {exec_time}ms' if exec_time else ''}")


def handle_task_error(exec_time: int, task: ServiceTask, error: Dict[str, Any], client_info: Dict[str, str]) -> None:
    service_name = client_info['service_name']
    client_id = client_info['client_id']

    LOGGER.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
                f"failed to complete task {f' in {exec_time}ms' if exec_time else ''}")

    # Add timestamps for creation, archive and expiry
    error['created'] = now_as_iso()
    error['archive_ts'] = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
    if task.ttl:
        error['expiry_ts'] = now_as_iso(task.ttl * 24 * 60 * 60)

    error = Error(error)
    error_key = error.build_key(service_tool_version=error.response.service_tool_version, task=task)
    dispatch_client.service_failed(task.sid, error_key, error)

    # Metrics
    if error.response.status == 'FAIL_RECOVERABLE':
        export_metrics_once(service_name, Metrics, dict(fail_recoverable=1), host=client_id, counter_type='service')
    else:
        export_metrics_once(service_name, Metrics, dict(fail_nonrecoverable=1), host=client_id, counter_type='service')
