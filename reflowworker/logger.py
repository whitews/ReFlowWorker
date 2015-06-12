import sys
import logging
from logging.handlers import RotatingFileHandler

from settings import WORKER_LOG, LOG_FORMAT

# setup logging
try:
    logger = logging.getLogger('Worker')
    logger.setLevel(level=logging.DEBUG)
    handler = RotatingFileHandler(
        WORKER_LOG,
        maxBytes=1024 * 1024,  # 1MB
        backupCount=7
    )
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
except (OSError, IOError) as e:
    message = "\nFailed to initialize log file: %s\n" + \
        "Do you have permission to write to this file?\n\n"
    sys.stderr.write(message % WORKER_LOG)
    sys.exit(str(e) + "\n")