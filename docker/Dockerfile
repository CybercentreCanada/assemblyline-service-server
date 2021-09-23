ARG branch=latest
ARG base=cccs/assemblyline-core
FROM $base:$branch
ARG version

# Install assemblyline UI
COPY setup.py dist* dist/
RUN pip install --no-cache-dir -f dist --user assemblyline-service-server==$version && rm -rf ~/.cache/pip

# run the app
CMD ["gunicorn", "assemblyline_service_server.patched:app", "--config=python:assemblyline_service_server.gunicorn_config", "--worker-class", "gevent"]
