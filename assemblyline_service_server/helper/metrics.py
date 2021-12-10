from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.service_heartbeat import Metrics

METRICS_FACTORIES = {}


def get_metrics_factory(service_name):
    if service_name in METRICS_FACTORIES:
        return METRICS_FACTORIES[service_name]

    factory = MetricsFactory('service', Metrics, name=service_name, export_zero=False)
    METRICS_FACTORIES[service_name] = factory
