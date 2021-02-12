ARG branch=latest
FROM cccs/assemblyline-core:$branch
ARG version

# Install assemblyline UI
RUN pip install --no-cache-dir --user assemblyline-service-server==$version && rm -rf ~/.cache/pip

# run the app
CMD ["gunicorn", "assemblyline_service_server.app:app", "--config=python:assemblyline_service_server.gunicorn_config"]
