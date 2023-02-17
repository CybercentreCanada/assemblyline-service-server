import logging

from elasticapm.contrib.flask import ElasticAPM
from flask import Flask
from flask.logging import default_handler
from os import environ, path

from assemblyline.common import forge, log as al_log
from assemblyline_service_server.api.v1.file import file_api
from assemblyline_service_server.api.v1.service import service_api
from assemblyline_service_server.api.v1.task import task_api
from assemblyline_service_server.api.v1.safelist import safelist_api
from assemblyline_service_server.healthz import healthz

config = forge.get_config()
CERT_BUNDLE = (
    # If internal encryption is enabled on deployment
    environ.get('SERVICE_SERVER_CLIENT_CERT_PATH', '/etc/assemblyline/ssl/service-server/tls.crt'),
    environ.get('SERVICE_SERVER_CLIENT_KEY_PATH', '/etc/assemblyline/ssl/service-server/tls.key')
)

# Prepare the logger
al_log.init_logging('svc')
LOGGER = logging.getLogger('assemblyline.svc_server')
LOGGER.info("Service server ready to receive connections...")
ssl_context = None
if all([path.exists(fp) for fp in CERT_BUNDLE]):
    # If all files required are present, start up encrypted comms
    ssl_context = CERT_BUNDLE

# Prepare the app
app = Flask('svc-server')
app.config['SECRET_KEY'] = config.ui.secret_key

app.register_blueprint(healthz)
app.register_blueprint(file_api)
app.register_blueprint(service_api)
app.register_blueprint(task_api)
app.register_blueprint(safelist_api)

# Setup logging
app.logger.setLevel(LOGGER.getEffectiveLevel())
app.logger.removeHandler(default_handler)
for ph in LOGGER.parent.handlers:
    app.logger.addHandler(ph)

# Setup APMs
if config.core.metrics.apm_server.server_url is not None:
    app.logger.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
    ElasticAPM(app, client=forge.get_apm_client("al_svc_server"))


if __name__ == '__main__':
    wlog = logging.getLogger('werkzeug')
    wlog.setLevel(LOGGER.getEffectiveLevel())
    for h in LOGGER.parent.handlers:
        wlog.addHandler(h)

    app.run(host='0.0.0.0', port=5003, debug=config.logging.log_level == 'DEBUG', ssl_context=ssl_context)
