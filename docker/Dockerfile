FROM cccs/assemblyline:latest
ARG version

# Install assemblyline UI
RUN pip3 install --user assemblyline-service-server==$version && rm -rf ~/.cache/pip

# Switch back to assemblyline and run the app
CMD ["gunicorn", "-b", ":5003", "-w", "1", "-k", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", "assemblyline_service_server.app:app"]
