FROM cccs/assemblyline:latest

ARG version

# Switch to root to install dependancies
USER root

# Install assemblyline UI
RUN pip3 install assemblyline-service-server==$version

# clear pip cache
RUN rm -rf ~/.cache/pip

# Switch back to assemblyline and run the app
USER assemblyline
CMD ["gunicorn", "-b", ":5003", "-w", "1", "-k", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", "assemblyline_service_server.app:app"]
