ARG branch=latest
FROM cccs/assemblyline:$branch
ARG version

# Install assemblyline UI
RUN pip install --no-cache-dir --user assemblyline-service-server==$version && rm -rf ~/.cache/pip
RUN if [ "${branch}" = "latest" ]; then pip install --user --upgrade --pre assemblyline assemblyline-core; fi

# run the app
CMD ["gunicorn", "assemblyline_service_server.app:app", "--config=python:assemblyline_service_server.gunicorn_config"]
