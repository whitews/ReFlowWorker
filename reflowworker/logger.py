import sys
import logging
from logging.handlers import RotatingFileHandler

WORKER_LOG = '/var/log/reflow_worker.log'
FORMAT = '%(levelname)s: %(asctime)-15s %(name)s: %(message)s'

# setup logging
try:
    logger = logging.getLogger('Worker')
    logger.setLevel(level=logging.DEBUG)
    handler = RotatingFileHandler(
        WORKER_LOG,
        maxBytes=1024 * 1024,  # 1MB
        backupCount=7
    )
    formatter = logging.Formatter(FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
except IOError, e:
    message = "Failed to setup logging to file: %s\n" + \
        "Do you have permission to write to this file?\n"
    sys.stderr.write(message % WORKER_LOG)
    sys.stderr.write(str(e))
    sys.exit(1)