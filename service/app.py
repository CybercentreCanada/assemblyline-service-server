import logging

from flask import Flask

from service.api.base import api
from service.api.v1 import apiv1
from service.api.v1.help import help_api
from service.api.v1.task import task_api

app = Flask("alsvc")
app.logger.setLevel(60)  # This completely turns off the flask logger

app.register_blueprint(api)
app.register_blueprint(apiv1)
app.register_blueprint(help_api)
app.register_blueprint(task_api)


def main():
    app.logger.setLevel(logging.INFO)
    app.jinja_env.cache = {}
    app.run(host="0.0.0.0", port=5003, debug=False)


if __name__ == '__main__':
    main()
