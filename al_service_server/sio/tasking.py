import hashlib
import json
import logging
import random
import threading
import time

from flask import request

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name
from al_service_server.sio.base import BaseNamespace, authenticated_only, LOGGER, get_request_id
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.service_timing_heartbeat import Metrics as TimingMetrics
from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service_client import ServiceClient, Current
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue

config = forge.get_config()
datastore = forge.get_datastore()
filestore = forge.get_filestore()


class TaskingNamespace(BaseNamespace):
    def __init__(self, namespace=None):
        self.watch_threads = set()
        self.dispatch_client = DispatchClient(datastore)
        super().__init__(namespace=namespace)

        # A lock to prevent the background metrics thread from clashing with the foreground
        # especially when writing values in client_info
        self._metrics_lock = threading.Lock()
        self._metrics_times = {}

        # A background thread that continues to push metrics about client busy/idle status
        # while the foreground thread is blocking on client events
        metrics_thread = threading.Thread(target=self._do_reports_between_events)
        metrics_thread.daemon = True
        metrics_thread.start()

        self._redis = get_client(
            db=config.core.redis.nonpersistent.db,
            host=config.core.redis.nonpersistent.host,
            port=config.core.redis.nonpersistent.port,
            private=False,
        )

        # A background thread that periodically checks the status of all the services
        # and cleans up any service task queues when a service is disabled/deleted
        cleanup_thread = threading.Thread(target=self._cleanup_service_tasks)
        cleanup_thread.daemon = True
        cleanup_thread.start()

    def on_disconnect(self):
        """When disconnecting we also need to stop the metrics counters.

        Otherwise they keep running in the background and the client is seen as still
        active by the rest of the system.
        """
        client_info = {}
        with self.connections_lock:
            client_id = get_request_id(request)
            if client_id in self.clients:
                client_info = self.clients[client_id]

        with self._metrics_lock:
            self._metrics_times.pop(client_id, None)
            if client_info.tasking_counters:
                client_info.tasking_counters[0].stop()
                client_info.tasking_counters[1].stop()

        super().on_disconnect()

    def _cleanup_service_tasks(self):
        """A daemon that cleans up tasks from the service queues when a service is disabled/deleted.

        When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
        emptied. The status of all the services will be periodically checked and any service that is found to be
        disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
        had an error.
        """
        # Get an initial list of all the service queues
        service_queues = {queue.decode('utf-8').lstrip('service-queue-'): None for queue in self._redis.keys(service_queue_name('*'))}

        while True:
            # Reset the status of the service queues
            service_queues = {service_name: False for service_name in service_queues}

            # Update the service queue status based on current list of services
            for service in datastore.list_all_services(full=True):
                service_queues[service.name] = service

            for service_name, service in service_queues.items():
                if not service or not service.enabled:
                    queue = NamedQueue(service_name, private=True)
                    while queue.length() != 0:
                        task, _ = self.dispatch_client.request_work(service.name, blocking=False)
                        error = Error(dict(
                            created='NOW',
                            expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60),
                            response=dict(
                                message='',
                                service_name=task.service_name,
                                service_version=service.version or ' ',
                                status='FAIL_NONRECOVERABLE',
                            ),
                            sha256=task.fileinfo.sha256,
                            type="TASK PRE-EMPTED",
                        ))

                        service_tool_version_hash = ''
                        task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                        conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
                        error_key = error.build_key(conf_key)

                        self.dispatch_client.service_failed(task.sid, error_key, error)

            # Wait 1 min before checking status of all services again
            time.sleep(60)

    def _get_counters(self, client_info: ServiceClient):
        """Each pair of metrics objects must have a correspondence with a client.

        Creating extra metrics objects causes the rest of the system to see extra false
        client instances if they aren't cleaned up, or not get consistent heartbeats
        if they are cleaned up.
        """
        with self._metrics_lock:
            if not client_info.tasking_counters:
                service_name = client_info.service_name
                client_info.tasking_counters = (
                    MetricsFactory('service', Metrics, name=service_name, config=config),
                    MetricsFactory('service_timing', TimingMetrics, name=service_name, config=config),
                )
            return client_info.tasking_counters

    def _do_reports_between_events(self):
        """A daemon that continues to repeat the last status message for all clients.

        This provides real time feedback for the metrics engine even when the task takes
        longer to execute than the metrics export interval.
        """
        while True:
            try:
                logging.getLogger('assemblyline.counters').setLevel(logging.INFO)
                time.sleep(1)
                with self.connections_lock:
                    client_info_list = list(self.clients.values())
                    # LOGGER.debug(f"Doing background reporting round on {len(client_info_list)} clients")
                    for client_info in client_info_list:
                        if client_info.client_id in self.banned_clients:
                            self.report_active(client_info)
                        elif client_info.client_id in self.available_clients.get(client_info.service_name, {}):
                            self.report_idle(client_info)
            except Exception:
                LOGGER.exception('Report thread suffered an error')

    def report_idle(self, client_info: ServiceClient):
        """Tell the metrics system that this client has been idle since the last report."""
        self._report_metrics(client_info, 'idle')

    def report_active(self, client_info: ServiceClient):
        """Tell the metrics system that this client has been busy since the last report."""
        self._report_metrics(client_info, 'execution')

    def _report_metrics(self, client_info: ServiceClient, timer_label):
        _, counter_timing = self._get_counters(client_info)
        with self._metrics_lock:
            now = time.time()
            delta = now - self._metrics_times.get(client_info.client_id, now)
            self._metrics_times[client_info.client_id] = now
            counter_timing.increment_execution_time(timer_label, delta)

    # noinspection PyBroadException
    def get_task_for_service(self, client_info: ServiceClient):
        service_name = client_info.service_name
        service_version = client_info.service_version
        service_tool_version = client_info.service_tool_version

        with self.connections_lock:
            if service_name in self.watch_threads:
                LOGGER.debug(f"Service {service_name} already has a watcher thread, exiting.")
                return
            self.watch_threads.add(service_name)

        LOGGER.info(f"SocketIO:{self.namespace} - Starting to monitor {service_name} queue for new tasks")
        queue = NamedQueue(service_queue_name(service_name), private=True)
        counter, _ = self._get_counters(client_info)

        try:
            while True:
                # Request a service task
                task, first_issue = self.dispatch_client.request_work(service_name, timeout=1)

                if not task:
                    # No task found in service queue
                    continue

                counter.increment('execute')

                if service_tool_version is not None:
                    service_tool_version_hash = hashlib.md5((service_tool_version.encode('utf-8'))).hexdigest()
                else:
                    service_tool_version_hash = ''
                task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

                result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                                   service_name=service_name,
                                                   service_version=service_version,
                                                   conf_key=conf_key)

                # If we are allowed to try to process the task from the cache.
                if not task.ignore_cache:
                    result = datastore.result.get_if_exists(result_key)
                    if result:
                        self.dispatch_client.service_finished(task.sid, result_key, result)
                        continue

                # This is not the first time request_work has given us this task
                if not first_issue:
                    # Check if this task is currently running in a client
                    for client in self.clients.values():
                        if client.current.status == 'PROCESSING' and task.sid == client.current.task_sid:
                            # Task is currently being processed by a client
                            # Check if this task has timed out
                            if now_as_iso() < client.current.task_timeout:
                                # Task has not yet timed out
                                # Continue and do nothing with the task
                                continue

                # No luck with the cache, lets dispatch the task to a client
                counter.increment('cache_miss')

                # Choose service client from the latest list of all available clients
                with self.connections_lock:
                    clients = list(set(self.available_clients.get(service_name, [])).difference(set(self.banned_clients)))
                    if len(clients) == 0:
                        # We have no more client, put the task back and quit...
                        if task:
                            queue.unpop(task.as_primitives())
                        break

                    client_id = random.choice(clients)

                    # Send the task to the service client
                    self.socketio.emit('got_task', task.as_primitives(), namespace=self.namespace, room=client_id)

                    # Add the service client to the list of banned clients, so that it doesn't receive anymore tasks
                    self.banned_clients.append(client_id)

                    service_timeout = self.clients[client_id].service_timeout
                    self.clients[client_id].current = Current(dict(
                        status='PROCESSING',
                        task_sid=task.sid,
                        task_timeout=now_as_iso(service_timeout),
                    ))

                LOGGER.info(f"SocketIO:{self.namespace} - {client_id} - "
                            f"Sending {service_name} service task to client")

        except Exception:
            LOGGER.exception(f"SocketIO:{self.namespace}")
        finally:
            if service_name in self.watch_threads:
                self.watch_threads.remove(service_name)

            LOGGER.info(f"SocketIO:{self.namespace} - No more clients connected to "
                        f"{service_name} service queue, exiting thread...")

    @authenticated_only
    def on_done_task(self, exec_time: int, task: dict, result: dict, client_info: ServiceClient):
        service_name = 'unknown'
        try:
            service_name = client_info.service_name
            counter, _ = self._get_counters(client_info)
            task = Task(task)

            if 'result' in result:  # Task completed successfully
                LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                            f"Client successfully completed the {service_name} task in {exec_time}ms")

                result = Result(result)

                if result.response.service_tool_version is not None:
                    service_tool_version_hash = hashlib.md5((result.response.service_tool_version.encode('utf-8'))).hexdigest()
                else:
                    service_tool_version_hash = ''
                task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
                result_key = result.build_key(conf_key)
                self.dispatch_client.service_finished(task.sid, result_key, result)

                # Metrics
                if result.result.score > 0:
                    counter.increment('scored')
                else:
                    counter.increment('not_scored')
            else:  # Task failed
                LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                            f"Client failed to complete the {service_name} task in {exec_time}ms")

                error = Error(result)

                if error.response.service_tool_version is not None:
                    service_tool_version_hash = hashlib.md5((error.response.service_tool_version.encode('utf-8'))).hexdigest()
                else:
                    service_tool_version_hash = ''
                task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()

                error_key = error.build_key(conf_key)
                self.dispatch_client.service_failed(task.sid, error_key, error)

                # Metrics
                if error.response.status == 'FAIL_RECOVERABLE':
                    counter.increment('fail_recoverable')
                else:
                    counter.increment('fail_nonrecoverable')

            self.report_active(client_info)
        except:
            LOGGER.exception(f"Error receiving result from: {service_name}")
            raise

    @authenticated_only
    def on_got_task(self, idle_time, client_info: ServiceClient):
        service_name = client_info.service_name
        self.report_idle(client_info)

        LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                    f"Client was idle for {idle_time}ms and received the {service_name} task and started processing")
        self._deactivate_client(client_info.client_id)

    @authenticated_only
    def on_wait_for_task(self, client_info: ServiceClient):
        LOGGER.info(f"SocketIO:{self.namespace} - {client_info.client_id} - "
                    f"Waiting for tasks in {client_info.service_name} service queue...")

        self._activate_client(client_info)

        self.socketio.start_background_task(target=self.get_task_for_service, client_info=client_info)

        self.clients[client_info.client_id].current = Current(dict(
            status='WAITING',
            task_sid=None,
            task_timeout=None,
        ))
