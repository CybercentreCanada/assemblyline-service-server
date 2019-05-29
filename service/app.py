try:
    from gevent.monkey import patch_all
    patch_all()
except ImportError:
    patch_all = None

import logging

from flask import Flask
from flask.logging import default_handler
from flask_socketio import SocketIO

from service import config
from service.sio.helper import HelperNamespace
from service.sio.tasking import TaskingNamespace

app = Flask("alsvc")
app.logger.setLevel(60)  # This completely turns off the flask logger

socketio = SocketIO(app, async_mode="gevent" if not config.DEBUG else "threading")
socketio.on_namespace(HelperNamespace('/helper'))
socketio.on_namespace(TaskingNamespace('/tasking'))

config.LOGGER.info("Service server API ready for connections...")


def main():
    wlog = logging.getLogger('werkzeug')
    wlog.setLevel(config.LOGGER.getEffectiveLevel())
    app.logger.setLevel(config.LOGGER.getEffectiveLevel())
    app.logger.removeHandler(default_handler)
    for h in config.LOGGER.parent.handlers:
        app.logger.addHandler(h)
        wlog.addHandler(h)

    app.jinja_env.cache = {}
    host = '0.0.0.0'
    port = '5003'
    config.LOGGER.info(f"Listening on http://{host}:{port} ...")
    socketio.run(app, host=host, port=int(port), debug=False)


if __name__ == '__main__':
    main()
