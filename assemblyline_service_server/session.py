from datetime import datetime
from typing import Optional as Opt
from assemblyline import odm
from assemblyline.odm.messages.task import Task

STATUSES = {'INITIALIZING', 'WAITING', 'PROCESSING', 'IDLE'}


@odm.model()
class Current(odm.Model):
    """The current assignment for a service worker"""
    status: str = odm.Enum(values=STATUSES, default='INITIALIZING')  # Status of the client
    task: Opt[Task] = odm.Optional(odm.Compound(Task))
    task_timeout: Opt[datetime] = odm.Optional(odm.Date())           # Time the task was assigned to the client


@odm.model()
class ServiceClient(odm.Model):
    """Session data for a service worker.

    This is not saved in the datastore, and its not shared with any other components
    """
    client_id: str = odm.Keyword()                           # Session ID of the client
    container_id: str = odm.Keyword()                        # Docker container ID of the client
    ip: str = odm.IP()                                       # IP address of the client
    service_name: str = odm.Keyword()                        # Name of the service running on the client
    service_version: str = odm.Keyword()                     # Version of the service running on the client
    service_tool_version: Opt[str] = odm.Optional(odm.Keyword())  # Tool version of the service running on the client
    service_timeout: int = odm.Integer()                          # Timeout of the service running on the client
    current: Opt[Current] = odm.Optional(odm.Compound(Current))   # Info about the current status and task assigned
    tasking_counters = odm.Optional(odm.Any())                    # MetricsFactory counters for the service
