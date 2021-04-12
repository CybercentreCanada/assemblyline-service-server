from os import environ as env
import multiprocessing

# Gunicorn config
bind = f":{int(env.get('PORT', 5003))}"
workers = int(env.get('WORKERS', multiprocessing.cpu_count()))
threads = int(env.get('THREADS', 4))
max_requests = int(env.get('MAX_REQUESTS', '1000'))

# Set the timeouts for service server workers to be ~double what the poll length for services should be.
graceful_timeout = 60
timeout = 60
