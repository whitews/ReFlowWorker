import json
import logging
import sys
import time
import multiprocessing

import pycuda.driver as cuda

from reflowrestclient import utils

from daemon import Daemon
from worker_process import WorkerProcess


WORKER_CONF = '/etc/reflow_worker.conf'
WORKER_LOG = '/var/log/reflow_worker.log'
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

        # setup logging
        try:
            logging.basicConfig(
                filename=WORKER_LOG,
                filemode='a',
                level=logging.DEBUG)
        except IOError, e:
            message = "Failed to setup logging to file: %s\n" + \
                "Do you have permission to write to this file?"
            sys.stderr.write(message % WORKER_LOG)
            sys.stderr.write(e.message)
            sys.exit(1)

        # All worker configs are stored in /etc/reflow-worker.conf
        try:
            worker_json = json.load(open(WORKER_CONF, 'r'))
        except Exception as e:
            logging.error(
                "Caught exception while opening %s" %
                WORKER_CONF)
            logging.exception("%s" % e.message)
            sys.exit(1)

        # look for the list of CUDA devices in config file &
        # test the device numbers as valid CUDA devices
        try:
            cuda.init()
            for device in worker_json['devices']:
                cuda.Device(device)
                self.devices[device] = None  # not currently working
        except Exception as e:
            logging.warning("Exception: %s", e.message)
            message = "No devices found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("Exiting since device list not found")
            sys.exit(1)

        # look for the host in config file
        if 'host' in worker_json:
            self.host = worker_json['host']
        else:
            message = "Host not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("Exiting since host not found")
            sys.exit(1)

        # look for the worker name in config file
        if 'name' in worker_json:
            self.name = worker_json['name']
        else:
            message = "Worker name not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("Exiting since worker name not found")
            sys.exit(1)

        # look for the worker token in config file
        # the token is the Worker's identifier to the host (i.e. password)
        if 'token' in worker_json:
            self.token = worker_json['token']
        else:
            message = "Worker token not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("Exiting since worker token not found")
            sys.exit(1)

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
            if result['data']['worker'] is not True:
                raise Exception
        except Exception, e:
            message = "Could not verify worker %s with host %s\n"
            logging.error(message % (self.name, self.host))
            logging.error(e.message)
            logging.error("Exiting since worker credentials are invalid")
            sys.exit(1)

        # Put the PID file in /tmp
        pid_file = '/tmp/reflow-worker-%s.pid' % self.name

        super(Worker, self).__init__(pid_file)

    def _run(self):
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

            print self.devices
            if len(working_requests) < len(self.devices):
                self.launch_workers()
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
                    utils.request_pr_assignment(
                        self.host,
                        self.token,
                        request['id'],
                        method=self.method
                    )
                    available_devices.pop()
            except Exception as e:
                logging.warning("Exception: ", e.message)
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

        except Exception as e:
            logging.warning("Exception: %s", e.message)
            return

if __name__ == "__main__":
    usage = "usage: %s start|stop|restart" % sys.argv[0]

    worker = Worker()

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            worker.start(debug=True)
        elif 'stop' == sys.argv[1]:
            worker.stop()
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
