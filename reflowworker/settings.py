# Worker configuration file in JSON format
WORKER_CONF = '/etc/reflow_worker.conf'

# Time (in seconds) between polling the ReFlow server for available work
DEFAULT_SLEEP = 15

# Settings for logging messages and errors
WORKER_LOG = '/var/log/reflow_worker.log'
LOG_FORMAT = '%(levelname)s: %(asctime)-15s %(name)s: %(message)s'

# Directory to store cached data for processing
CACHE_DIR = '/var/tmp/ReFlow-data/'
