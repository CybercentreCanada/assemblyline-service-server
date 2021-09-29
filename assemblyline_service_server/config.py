import logging
import os

from assemblyline.common import forge
from assemblyline.common import log as al_log
from assemblyline.common.version import BUILD_MINOR, FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.remote.datatypes.counters import Counters

config = forge.get_config()

#################################################################
# Configuration

CLASSIFICATION = forge.get_classification()
DEBUG = config.ui.debug
VERSION = os.environ.get('ASSEMBLYLINE_VERSION', f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0")
AUTH_KEY = os.environ.get('SERVICE_API_AUTH_KEY', 'ThisIsARandomAuthKey...ChangeMe!')

RATE_LIMITER = Counters(prefix="quota",
                        host=config.core.redis.nonpersistent.host,
                        port=config.core.redis.nonpersistent.port,
                        track_counters=True)

# End of Configuration
#################################################################

#################################################################
# Prepare loggers
config.logging.log_to_console = config.logging.log_to_console or DEBUG
al_log.init_logging('svc', config=config)

LOGGER = logging.getLogger('assemblyline.svc')

LOGGER.debug('Logger ready!')

# End of prepare logger
#################################################################

#################################################################
# Global instances
STORAGE = forge.get_datastore(config=config)
FILESTORE = forge.get_filestore(config=config)

# End global
#################################################################
