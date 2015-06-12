import json
import sys
import time
import multiprocessing

import pycuda.driver as cuda

from reflowrestclient import utils

from daemon import Daemon
from logger import logger
from worker_process import WorkerProcess


WORKER_CONF = '/etc/reflow_worker.conf'
DEFAULT_SLEEP = 15  # in seconds


class Worker(Daemon):
    """
    The Worker runs as a background process, much like a service, so try
    VERY hard not to exit unless there's a catastrophic failure like the
    worker config file is malformed. In other cases, just capture and log
    all Exceptions and Errors
    """
    def __init__(self):
        # a Worker can have only one host
        self.host = None
        self.name = None
        self.token = None
        # dictionary of devices where the key is the GPU device ID, and
        # the value is the ProcessRequest PK if the device is working, or
        # None if the device is free
        self.devices = {}

        # All worker configs are stored in /etc/reflow-worker.conf
        # Exit on any Exception. if we can't open or read the configuration,
        # we cannot continue
        # noinspection PyBroadException
        try:
            worker_json = json.load(open(WORKER_CONF, 'r'))
        except Exception:
            logger.error(
                "Caught exception while opening %s",
                WORKER_CONF,
                exc_info=True
            )
            sys.exit("Worker failed to start, see log file for details")

        # look for the list of CUDA devices in config file &
        # test the device numbers as valid CUDA devices
        # Exit on any Exception. If we do not have any CUDA devices,
        # we cannot continue
        # noinspection PyBroadException
        try:
            cuda.init()
            for device in worker_json['devices']:
                cuda.Device(device)
                self.devices[device] = None  # not currently working
        except Exception:
            logger.error(
                "Device list not found in config file:  %s",
                WORKER_CONF,
                exc_info=True
            )
            sys.exit("Worker failed to start, see log file for details")

        # look for the host & worker name in config file
        try:
            self.host = worker_json['host']
            self.name = worker_json['name']
            self.token = worker_json['token']
        except Exception:
            logger.error(
                "Errors in config file:  %s",
                 WORKER_CONF,
                exc_info=True
            )
            sys.exit("Worker failed to start, see log file for details")

        # look for the server protocol, http or https
        if 'method' in worker_json:
            self.method = utils.METHOD[worker_json['method']]
        else:
            # default is https
            self.method = utils.METHOD['https']

        # verify worker with the host
        # catching all exceptions here, since if anything goes wrong
        # we should not continue
        try:
            result = utils.verify_worker(
                self.host,
                self.token,
                method=self.method
            )
            is_valid = result['data']['worker']
            assert is_valid  # value should be True
        except Exception:
            message = "Could not verify worker %s with host %s\n"
            logger.error(
                "Could not verify worker '%s' with host '%s'",
                self.name,
                self.host,
                exc_info=True
            )
            sys.exit("Worker failed to start, see log file for details")

        # Put the PID file in /tmp
        pid_file = '/tmp/reflow-worker-%s.pid' % self.name

        super(Worker, self).__init__(pid_file)

    def _run(self):
        logger.info("Worker started")

        while True:
            # check in on our children
            working_requests = []
            for p in multiprocessing.active_children():
                if type(p) is WorkerProcess:
                    working_requests.append(
                        p.assigned_pr.process_request_id
                    )

            # free any devices that are no longer working
            for gpu_id in self.devices:
                if self.devices[gpu_id] not in working_requests:
                    self.devices[gpu_id] = None

            if len(working_requests) < len(self.devices):
                # launch workers first in case the server already has
                # work assigned to this worker
                self.launch_workers()

                # request assignements for any free GPU devices
                self.request_assignments()

            time.sleep(DEFAULT_SLEEP)

    def get_available_devices(self):
        available_devices = []
        for gpu_id in self.devices:
            if self.devices[gpu_id] is None:
                available_devices.append(gpu_id)
        return available_devices

    def request_assignments(self):
        available_devices = self.get_available_devices()

        if len(available_devices) > 0:
            try:
                viable_requests = utils.get_viable_process_requests(
                    self.host,
                    self.token,
                    method=self.method
                )
                for request in viable_requests['data']:
                    # Request assignments for the number of available devices
                    if len(available_devices) <= 0:
                        return

                    # request ProcessRequest assignment
                    logger.info(
                        "Requesting assignment of PR %s",
                        str(request["id"])
                    )
                    utils.request_pr_assignment(
                        self.host,
                        self.token,
                        request['id'],
                        method=self.method
                    )
                    available_devices.pop()
            except Exception:
                logger.error(
                    "Error trying to request assignments",
                    exc_info=True
                )
                return

    def launch_workers(self):
        # If we get here then there are devices available for processing.
        # First, see if the ReFlow server already has stuff assigned to us
        try:
            query_assignment_response = utils.get_assigned_process_requests(
                self.host,
                self.token,
                method=self.method
            )

            # iterate through assigned PRs
            for pr in query_assignment_response['data']:
                # check if the PR is already being worked on
                if pr['id'] in self.devices.values():
                    continue

                # see if we have an available device
                available_devices = self.get_available_devices()

                if len(available_devices) <= 0:
                        return

                gpu_id = available_devices.pop(0)
                process = WorkerProcess(
                    self.host,
                    self.token,
                    self.method,
                    pr['id'],
                    gpu_id
                )
                process.start()
                self.devices[gpu_id] = pr['id']

        except Exception:
            logger.error(
                    "Error trying to launch worker processes",
                    exc_info=True
                )
            return

if __name__ == "__main__":
    usage = "usage: %s start|stop|restart" % sys.argv[0]

    worker = Worker()

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            worker.start(debug=True)
        elif 'stop' == sys.argv[1]:
            worker.stop()
            logger.info("Worker stopped")
        elif 'restart' == sys.argv[1]:
            worker.restart()
        else:
            print "Unknown command"
            print usage
            sys.exit(2)
        sys.exit(0)
    else:
        print usage
        sys.exit(2)
