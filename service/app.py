try:
    from gevent.monkey import patch_all
    patch_all()
except ImportError:
    patch_all = None

import logging

from flask import Flask
from flask_socketio import SocketIO

from assemblyline.common import forge, log as al_log
from service.sio.helper import HelperNamespace
from service.sio.tasking import TaskingNamespace

config = forge.get_config()

# Prepare the logger
al_log.init_logging('svc')
LOGGER = logging.getLogger('assemblyline.svc.socketio')
LOGGER.info("SocketIO server ready to receive connections...")

# Prepare the app
app = Flask('svc-socketio')
app.config['SECRET_KEY'] = config.ui.secret_key
# NOTE: we need to run in threading mode while debugging otherwise, use gevent
socketio = SocketIO(app, async_mode='gevent' if not config.ui.debug else 'threading')

# Loading the different namespaces
socketio.on_namespace(HelperNamespace('/helper'))
socketio.on_namespace(TaskingNamespace('/tasking'))


if __name__ == '__main__':
    app.logger.setLevel(60)
    wlog = logging.getLogger('werkzeug')
    wlog.setLevel(60)
    # Run debug mode
    socketio.run(app, host='0.0.0.0', port=5003, debug=False)
