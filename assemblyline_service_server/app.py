import logging

from flask import Flask
from flask.logging import default_handler

from assemblyline.common import forge, log as al_log
from assemblyline_service_server.api.v1.file import file_api
from assemblyline_service_server.api.v1.service import service_api
from assemblyline_service_server.api.v1.task import task_api

config = forge.get_config()

# Prepare the logger
al_log.init_logging('svc')
LOGGER = logging.getLogger('assemblyline.svc_server')
LOGGER.info("Service server ready to receive connections...")

# Prepare the app
app = Flask('svc-server')
app.config['SECRET_KEY'] = config.ui.secret_key

app.register_blueprint(file_api)
app.register_blueprint(service_api)
app.register_blueprint(task_api)

# Setup logging
app.logger.setLevel(LOGGER.getEffectiveLevel())
app.logger.removeHandler(default_handler)
for ph in LOGGER.parent.handlers:
    app.logger.addHandler(ph)


if __name__ == '__main__':
    wlog = logging.getLogger('werkzeug')
    wlog.setLevel(LOGGER.getEffectiveLevel())
    for h in LOGGER.parent.handlers:
        wlog.addHandler(h)

    app.run(host='0.0.0.0', port=5003, debug=config.logging.log_level == 'DEBUG')
