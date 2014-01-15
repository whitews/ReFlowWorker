import json
import logging
import sys
import time

from reflowrestclient.processing.daemon import Daemon
from reflowrestclient import utils
from models import ProcessRequest
from processes import PROCESS_LIST

WORKER_CONF = '/etc/reflow_worker.conf'
WORKER_LOG = '/var/log/reflow_worker.log'
DEFAULT_SLEEP = 5  # in seconds


class Worker(Daemon):
    def __init__(self):
        # a Worker can have only one host
        self.host = None
        self.name = None
        self.token = None
        self.genuine = False
        self.assigned_pr = None
        self.errors = list()

        # default sleep time between checking the server (in seconds)
        self.sleep = DEFAULT_SLEEP

        # setup logging
        try:
            logging.basicConfig(
                filename=WORKER_LOG,
                filemode='w',
                level=logging.DEBUG)
        except IOError, e:
            message = "ERROR: Failed to setup logging to file: %s\n" + \
                "Do you have permission to write to this file?"
            sys.stderr.write(message % WORKER_LOG)
            sys.stderr.write(e.message)
            sys.exit(1)

        # All worker configs are stored in /etc/reflow-worker.conf
        try:
            worker_json = json.load(open(WORKER_CONF, 'r'))
        except Exception as e:
            logging.error(
                "ERROR: Caught exception while opening %s",
                WORKER_CONF)
            logging.error("EXCEPTION: ", e.message)
            sys.exit(1)

        # look for the host in config file
        if 'host' in worker_json:
            self.host = worker_json['host']
        else:
            message = "ERROR: Host not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("ERROR: Exiting since host not found")
            sys.exit(1)

        # look for the worker name in config file
        if 'name' in worker_json:
            self.name = worker_json['name']
        else:
            message = "ERROR: Worker name not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("ERROR: Exiting since worker name not found")
            sys.exit(1)

        # look for the worker token in config file
        # the token is the Worker's identifier to the host (i.e. password)
        if 'token' in worker_json:
            self.token = worker_json['token']
        else:
            message = "ERROR: Worker token not found in config file:  %s.\n"
            logging.error(message % WORKER_CONF)
            logging.error("ERROR: Exiting since worker token not found")
            sys.exit(1)

        # verify worker with the host
        # catching all exceptions here, since if anything goes wrong
        # we should not continue
        try:
            result = utils.verify_worker(self.host, self.token)
            self.genuine = result['data']['worker']  # should be True
            if self.genuine is not True:
                raise Exception
        except Exception, e:
            message = "ERROR: Could not verify worker %s with host %s\n"
            logging.error(message % (self.name, self.host))
            logging.error("ERROR: Caught exception: ", e.message)
            logging.error("ERROR: Exiting since worker credentials are invalid")
            sys.exit(1)

        # Put the PID file in /tmp
        pid_file = '/tmp/reflow-worker-%s.pid' % self.name

        super(Worker, self).__init__(pid_file)

    def _run(self):
        while True:
            self.__loop()

    def __loop(self):
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
                    "ERROR: Malformed response from ReFlow server attempting " +
                    "to get viable process requests.")
                time.sleep(self.sleep)
                return
            if not isinstance(viable_requests['data'], list):
                logging.warning(
                    "ERROR: Malformed response from ReFlow " +
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
                    time.sleep(self.sleep)
                    return
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
        time.sleep(self.sleep)

    def validate_inputs(self):
        """
        It will be called after the Worker is assigned a ProcessRequest.
        Returns True if the inputs are valid, else returns False
        """
        if self.assigned_pr.process_id in PROCESS_LIST:
            if PROCESS_LIST[self.assigned_pr.process_id] == 'Test':
                return True
            if PROCESS_LIST[self.assigned_pr.process_id] == 'HDP':
                # TODO: check the inputs here
                # should have inputs:
                #     n_clusters
                #     n_iterations
                #     burn_in
                #     logicle_t
                #     logicle_w
                pass
        return False

    def process(self):
        """
        It will be called after when the Worker has an assigned ProcessRequest.
        Define all the processing tasks here.
        """
        if self.assigned_pr.process_id in PROCESS_LIST:
            if PROCESS_LIST[self.assigned_pr.process_id] == 'Test':
                return True
        return False

    def report_errors(self):
        """
        It will be called after process() if that method returned False
        """
        return

    def upload_results(self):
        """
        It will be called after process() if that method returns successfully
        """
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