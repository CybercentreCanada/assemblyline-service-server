FROM cccs/assemblyline:latest

ARG version

# Switch to root to install dependancies
USER root

# Install assemblyline UI
RUN pip3 install assemblyline-service-server==$version

# Switch back to assemblyline and run the app
USER assemblyline
CMD ["gunicorn", "-b", ":5003", "-w", "1", "-k", "gevent", "al_service_server.app:app"]
