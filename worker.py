import json
import logging
import sys
import time

import abc

from reflowrestclient.processing.daemon import Daemon
from reflowrestclient import utils
from models import ProcessRequest

WORKER_CONF = '/etc/reflow_worker.conf'


class Worker(Daemon):
    __metaclass__ = abc.ABCMeta

    def __init__(self, host, name, sleep=300):
        # a Worker can have only one host
        self.host = host
        self.name = name
        self.assigned_pr = None
        self.errors = list()

        # default sleep time between checking the server (in seconds)
        self.sleep = sleep

        # the token is the Worker's identifier to the host (i.e. password)
        # but it is not stored in the source code
        # All worker tokens on a system are stored in /etc/reflow-worker.conf
        try:
            worker_json = json.load(open(WORKER_CONF, 'r'))
            self.token = worker_json[host][name]
            del worker_json
        except Exception as e:
            message = "No token found for worker. Check the config file %s\n"
            sys.stderr.write(message % WORKER_CONF)
            sys.stderr.write(e.message)
            sys.exit(1)

        # verify worker with the host
        try:
            result = utils.verify_worker(self.host, self.token)
            self.genuine = result['data']['worker']  # should be True
            if self.genuine is not True:
                raise Exception
        except Exception as e:
            message = "Could not verify worker %s with host %s\n"
            sys.stderr.write(message % (self.name, self.host))
            sys.stderr.write(e.message)
            sys.exit(1)

        # Put the PID file in /tmp
        pid_file = '/tmp/reflow-worker-%s.pid' % self.name

        super(Worker, self).__init__(pid_file)

    def run(self):
        logging.basicConfig(
            filename='/Users/swhite/Desktop/worker.log',
            filemode='w',
            level=logging.DEBUG)

        while True:
            self.loop()

    def loop(self):
        # Once inside the loop, try VERY hard not to exit,
        # just capture and log all Exceptions and Errors
        if self.assigned_pr is None:
            try:
                viable_requests = utils.get_viable_process_requests(
                    self.host,
                    self.token)
            except Exception as e:
                logging.warning("Exception: ", e.message)
                time.sleep(self.sleep)
                return

            if not 'data' in viable_requests:
                logging.warning(
                    "Error: Malformed response from ReFlow server attempting " +
                    "to get viable process requests.")
                time.sleep(self.sleep)
                return
            if not isinstance(viable_requests['data'], list):
                logging.warning(
                    "Error: Malformed response from ReFlow " +
                    "server attempting to get viable process requests.")
                time.sleep(self.sleep)
                return

            if not len(viable_requests['data']) > 0:
                time.sleep(self.sleep)
                return

            for request in viable_requests['data']:
                # request ProcessRequest assignment
                try:
                    assignment_response = utils.request_pr_assignment(
                        self.host,
                        self.token,
                        request['id'])
                except Exception as e:
                    logging.warning("Exception: ", e.message)

                if not 'status' in assignment_response:
                    continue
                if not assignment_response['status'] == 201:
                    continue

                # check the response,
                # if 201 then our assignment request was granted and
                # we'll verify we have the assignment
                try:
                    verify_assignment_response = utils.verify_pr_assignment(
                        self.host,
                        self.token,
                        request['id'])
                    if verify_assignment_response['data']['assignment']:
                        pr_response = utils.get_process_request(
                            self.host,
                            self.token,
                            request['id'])
                        self.assigned_pr = ProcessRequest(
                            self.host,
                            self.token,
                            pr_response['data'])
                except Exception as e:
                    logging.warning("Exception: ", e.message)
            else:
                time.sleep(self.sleep)
        else:
            # We've got something to do!
            are_inputs_valid = self.validate_inputs()

            if not are_inputs_valid:
                logging.warning(
                    "Error: Invalid input values for process request")
                self.report_errors()
                return

            # Download the samples
            assert isinstance(self.assigned_pr, ProcessRequest)
            self.assigned_pr.download_samples()

            # Stub method to process the data
            process_status = self.process()

            if not process_status:
                self.report_errors()

            # Verify assignment once again
            try:
                verify_assignment_response = utils.verify_pr_assignment(
                    self.host,
                    self.token,
                    self.assigned_pr.process_request_id)
                if not verify_assignment_response['data']['assignment']:
                    # we're not assigned anymore, delete our PR and return
                    self.assigned_pr = None
                    raise Exception("Server revoked our assignment")
            except Exception as e:
                logging.warning("Exception: ", e.message)
                return

            # Upload results
            try:
                self.upload_results()
            except Exception as e:
                logging.warning("Exception: ", e.message)
                return

            # Report the ProcessRequest is complete
            try:
                verify_complete_response = utils.complete_pr_assignment(
                    self.host,
                    self.token,
                    self.assigned_pr.process_request_id)
                if verify_complete_response['status'] != 200:
                    # something went wrong
                    raise Exception("Server rejected our 'Complete' request")
            except Exception as e:
                logging.warning("Exception: ", e.message)
                return

            # Verify 'Complete' status
            try:
                r = utils.get_process_request(
                    self.host,
                    self.token,
                    self.assigned_pr.process_request_id)
                if not 'data' in r:
                    raise Exception("Improper host response, no 'data' key")
                if not 'status' in r['data']:
                    raise Exception("Improper host response, no 'status' key")
                if r['data']['status'] != 'Complete':
                    raise Exception("Failed to mark assignment complete")
            except Exception as e:
                # TODO: should probably do more than just log an error
                # locally, perhaps try to send errors again? then re-try to
                # send complete status again?
                logging.warning("Exception: ", e.message)
                return

            # TODO: Clean up! Delete the local files

            # We're done, delete assignment and clear errors
            self.assigned_pr = None
            self.errors = list()

    @abc.abstractmethod
    def validate_inputs(self):
        """
        Override this method when subclassing Worker.
        It will be called after the Worker is assigned a ProcessRequest.
        Returns True if the inputs are valid, else returns False
        """
        return False

    @abc.abstractmethod
    def process(self):
        """
        Override this method when subclassing Worker.
        It will be called after when the Worker has an assigned ProcessRequest.
        Define all the processing tasks here.
        """
        return False

    @abc.abstractmethod
    def report_errors(self):
        """
        Override this method when subclassing Worker.
        It will be called after process() if that method returned False
        """
        return

    @abc.abstractmethod
    def upload_results(self):
        """
        Override this method when subclassing Worker.
        It will be called after process() if that method returns successfully
        """
        return