import logging
import os

from assemblyline.common import version
from assemblyline.common.logformat import AL_LOG_FORMAT
from assemblyline.common import forge
from assemblyline.remote.datatypes.counters import Counters
from assemblyline.common import log as al_log

config = forge.get_config()
    
#################################################################
# Configuration

CLASSIFICATION = forge.get_classification()
DEBUG = config.ui.debug
BUILD_MASTER = version.FRAMEWORK_VERSION
BUILD_LOWER = version.SYSTEM_VERSION
BUILD_NO = version.BUILD_MINOR

RATE_LIMITER = Counters(prefix="quota",
                        host=config.core.redis.nonpersistent.host,
                        port=config.core.redis.nonpersistent.port,
                        db=config.core.redis.nonpersistent.db,
                        track_counters=True)

# End of Configuration
#################################################################

#################################################################
# Prepare loggers
DEBUG = True
config.logging.log_to_console = config.logging.log_to_console or DEBUG
al_log.init_logging("alsvc", config=config, log_level=logging.DEBUG if DEBUG else logging.INFO)

LOGGER = logging.getLogger('assemblyline.alsvc')

LOGGER.debug('Logger ready!')

# End of prepare logger
#################################################################

#################################################################
# Global instances
STORAGE = forge.get_datastore()

# End global
#################################################################
