import time

from typing import cast, Dict, Any
from flask import request

from assemblyline.common import forge
from assemblyline.common.constants import SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common.dict_utils import flatten, unflatten
from assemblyline.common.forge import CachedObject
from assemblyline.common.heuristics import HeuristicHandler, InvalidHeuristicException
from assemblyline.common.isotime import now_as_iso
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
from assemblyline_service_server.config import FILESTORE, LOGGER, STORAGE, config
from assemblyline_service_server.helper.heuristics import get_heuristics

status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60*30)
dispatch_client = DispatchClient(STORAGE)
heuristics = cast(Dict[str, Heuristic], CachedObject(get_heuristics, refresh=300))
heuristic_hander = HeuristicHandler(STORAGE)
tag_safelister = CachedObject(forge.get_tag_safelister,
                              kwargs=dict(log=LOGGER, config=config, datastore=STORAGE),
                              refresh=300)

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
     'timeout': '30'}

    Result example:
    {'keep_alive': true}
    """
    service_name = client_info['service_name']
    service_version = client_info['service_version']
    service_tool_version = client_info['service_tool_version']
    client_id = client_info['client_id']
    remaining_time = timeout = int(float(request.headers.get('timeout', 30)))

    try:
        service_data = dispatch_client.service_data[service_name]
    except KeyError:
        return make_api_response({}, "The service you're asking task for does not exist, try later", 404)

    start_time = time.time()
    stats = {
        "execute": 0,
        "cache_miss": 0,
        "cache_hit": 0,
        "cache_skipped": 0,
        "scored": 0,
        "not_scored": 0
    }

    try:
        while remaining_time > 0:
            cache_found = False

            # Set the service status to Idle since we will be waiting for a task
            status_table.set(client_id, (service_name, ServiceStatus.Idle, start_time + timeout))

            # Getting a new task
            task = dispatch_client.request_work(client_id, service_name, service_version, timeout=remaining_time)

            if not task:
                # We've reached the timeout and no task found in service queue
                return make_api_response(dict(task=False))

            # We've got a task to process, consider us busy
            status_table.set(client_id, (service_name, ServiceStatus.Running, time.time() + service_data.timeout))
            stats['execute'] += 1

            result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                               service_name=service_name,
                                               service_version=service_version,
                                               service_tool_version=service_tool_version,
                                               is_empty=False,
                                               task=task)

            # If we are allowed, try to see if the result has been cached
            if not task.ignore_cache and not service_data.disable_cache:
                # Checking for previous results for this key
                result = STORAGE.result.get_if_exists(result_key)
                if result:
                    stats['cache_hit'] += 1
                    if result.result.score:
                        stats['scored'] += 1
                    else:
                        stats['not_scored'] += 1

                    result.archive_ts = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
                    if task.ttl:
                        result.expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)

                    dispatch_client.service_finished(task.sid, result_key, result)
                    cache_found = True

                if not cache_found:
                    # Checking for previous empty results for this key
                    result = STORAGE.emptyresult.get_if_exists(f"{result_key}.e")
                    if result:
                        stats['cache_hit'] += 1
                        stats['not_scored'] += 1
                        result = STORAGE.create_empty_result_from_key(result_key)
                        dispatch_client.service_finished(task.sid, f"{result_key}.e", result)
                        cache_found = True

                if not cache_found:
                    stats['cache_miss'] += 1
            else:
                stats['cache_skipped'] += 1

            if not cache_found:
                # No luck with the cache, lets dispatch the task to a client
                return make_api_response(dict(task=task.as_primitives()))

            # Recalculating how much time we have left before we reach the timeout
            remaining_time = start_time + timeout - time.time()

        # We've been processing cache hit for the length of the timeout... bailing out!
        return make_api_response(dict(task=False))
    finally:
        export_metrics_once(service_name, Metrics, stats, host=client_id, counter_type='service')


@task_api.route("/", methods=["POST"])
@api_login()
def task_finished(client_info):
    """
    Header:
    {'container_id': abcd...123
     'service_name': 'Extract',
     'service_version': '4.0.1',
     'service_tool_version': '
    }


    Data Block:
    {
     "exec_time": 300,
     "task": <Original Task Dict>,
     "result": <AL Result Dict>,
     "freshen": true
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
    archive_ts = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
    if task.ttl:
        expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
    else:
        expiry_ts = None

    # Check if all files are in the filestore
    if freshen:
        missing_files = []
        for f in result['response']['extracted'] + result['response']['supplementary']:
            cur_file_info = STORAGE.file.get_if_exists(f['sha256'], as_obj=False)
            if cur_file_info is None or not FILESTORE.exists(f['sha256']):
                missing_files.append(f['sha256'])
            else:
                cur_file_info['archive_ts'] = archive_ts
                cur_file_info['expiry_ts'] = expiry_ts
                cur_file_info['classification'] = f['classification']
                STORAGE.save_or_freshen_file(f['sha256'], cur_file_info,
                                             cur_file_info['expiry_ts'], cur_file_info['classification'],
                                             is_section_image=f.get('is_section_image', False))
        if missing_files:
            return missing_files

    service_name = client_info['service_name']
    client_id = client_info['client_id']

    # Add scores to the heuristics, if any section set a heuristic
    total_score = 0
    for section in result['result']['sections']:
        zeroize_on_sig_safe = section.pop('zeroize_on_sig_safe', True)
        section['tags'] = flatten(section['tags'])
        if section.get('heuristic'):
            heur_id = f"{client_info['service_name'].upper()}.{str(section['heuristic']['heur_id'])}"
            section['heuristic']['heur_id'] = heur_id
            try:
                section['heuristic'], new_tags = heuristic_hander.service_heuristic_to_result_heuristic(
                    section['heuristic'], heuristics, zeroize_on_sig_safe)
                for tag in new_tags:
                    section['tags'].setdefault(tag[0], [])
                    if tag[1] not in section['tags'][tag[0]]:
                        section['tags'][tag[0]].append(tag[1])
                total_score += section['heuristic']['score']
            except InvalidHeuristicException:
                section['heuristic'] = None

    # Update the total score of the result
    result['result']['score'] = total_score

    # Add timestamps for creation, archive and expiry
    result['created'] = now_as_iso()
    result['archive_ts'] = archive_ts
    result['expiry_ts'] = expiry_ts

    # Pop the temporary submission data
    temp_submission_data = result.pop('temp_submission_data', None)

    # Process the tag values
    for section in result['result']['sections']:
        # Perform tag safelisting
        tags, safelisted_tags = tag_safelister.get_validated_tag_map(section['tags'])
        section['tags'] = unflatten(tags)
        section['safelisted_tags'] = safelisted_tags

        section['tags'], dropped = construct_safe(Tagging, section.get('tags', {}))

        # Set section score to zero and lower total score if service is set to zeroize score
        # and all tags were safelisted
        if section.pop('zeroize_on_tag_safe', False) and \
                section.get('heuristic') and \
                len(tags) == 0 and \
                len(safelisted_tags) != 0:
            result['result']['score'] -= section['heuristic']['score']
            section['heuristic']['score'] = 0

        if dropped:
            LOGGER.warning(f"[{task.sid}] Invalid tag data from {client_info['service_name']}: {dropped}")

    result = Result(result)
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
