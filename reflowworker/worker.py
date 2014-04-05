import json
import logging
import sys
import time
import os

from reflowrestclient import utils

#utils.METHOD = 'http://'

from daemon import Daemon
from models import ProcessRequest
from clustering_processes import hdp

WORKER_CONF = '/etc/reflow_worker.conf'
WORKER_LOG = '/var/log/reflow_worker.log'
DEFAULT_SLEEP = 15  # in seconds


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

        # verify worker with the host
        # catching all exceptions here, since if anything goes wrong
        # we should not continue
        try:
            result = utils.verify_worker(self.host, self.token)
            self.genuine = result['data']['worker']  # should be True
            if self.genuine is not True:
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
            self.__loop()
            if self.assigned_pr is None:
                time.sleep(self.sleep)

    def __loop(self):
        # Once inside the loop, try, pun intended, VERY hard not to exit,
        # just capture and log all Exceptions and Errors

        # First, see if the ReFlow server already has stuff assigned to us
        try:
            query_assignment_response = utils.get_assigned_process_requests(
                self.host,
                self.token)
            if len(query_assignment_response['data']) > 0:
                # get the 1st PR in the list
                pr_response = utils.get_process_request(
                    self.host,
                    self.token,
                    query_assignment_response['data'][0]['id'])
                self.assigned_pr = ProcessRequest(
                    self.host,
                    self.token,
                    pr_response['data'])
        except Exception as e:
            logging.warning("Exception: ", e.message)
            return

        # If we don't already have an assignment, see if any work is available
        if self.assigned_pr is None:
            try:
                viable_requests = utils.get_viable_process_requests(
                    self.host,
                    self.token)
            except Exception as e:
                logging.warning("Exception: ", e.message)
                return

            if not 'data' in viable_requests:
                logging.warning(
                    "Malformed response from ReFlow server attempting " +
                    "to get viable process requests.")
                return
            if not isinstance(viable_requests['data'], list):
                logging.warning(
                    "Malformed response from ReFlow " +
                    "server attempting to get viable process requests.")
                return

            if not len(viable_requests['data']) > 0:
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
                        # we've got assignment, stop iterating over viable
                        break
                except Exception as e:
                    logging.warning("Exception: ", e.message)
                    return
        else:
            # We've got something to do!

            # First, validate the inputs
            if not self.assigned_pr.validate_inputs():
                logging.warning(
                    "Invalid input values for process request")
                self.report_errors()
                return

            # Download the samples
            assert isinstance(self.assigned_pr, ProcessRequest)
            self.assigned_pr.download_samples()

            # Stub method to process the data
            try:
                process_status = self.process()
            except Exception, e:
                logging.exception(e.message)
                self.report_errors()
                return

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
                logging.exception(e.message)
                return

            # Upload results
            try:
                self.upload_results()
            except Exception as e:
                logging.exception(e.message)
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
                logging.exception(e.message)
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
                logging.exception(e.message)
                return

            # TODO: Clean up! Delete the local files

            # We're done, delete assignment and clear errors
            self.assigned_pr = None
            self.errors = list()
            return

    def process(self):
        """
        Called after validate_inputs
        Calls to all the processing sub-tasks are made here.
        """

        # first we compensate
        self.assigned_pr.compensate_samples()

        # next is transformation
        if self.assigned_pr.transformation == 'logicle':
            self.assigned_pr.apply_logicle_transform()
        else:
            # for now only logicle is implemented, asinch will be added in the future
            return False

        # next is normalization of common sample parameters
        self.assigned_pr.normalize_transformed_samples()

        # next is clustering
        if self.assigned_pr.clustering == 'hdp':
            hdp(self.assigned_pr)
        else:
            # only HDP is implemented at this time
            return False

        return True

    def report_errors(self):
        """
        It will be called after process() if that method returned False
        """
        return

    def upload_results(self):
        """
        It will be called after process() if that method returns successfully
        """
        if self.assigned_pr is None:
            return

        # get list of files in the results dir for this PR id
        # all files in results will be uploaded to ReFlow as
        # ProcessOutputValue model instances, and these have 3 values:
        #     PR_id: the id of the ProcessRequest
        #     key: the name of a results file
        #     value: the file itself (usually JSON)
        results_files = list()
        results_dir = self.assigned_pr.results_directory
        for f in os.listdir(results_dir):
            if os.path.isfile(os.path.join(results_dir, f)):
                results_files.append(f)
        for f in results_files:
            utils.post_process_request_output(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                os.path.join(results_dir, f)
            )
        return

if __name__ == "__main__":
    usage = "usage: %s start|stop|restart" % sys.argv[0]

    worker = Worker()

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            worker.start(debug=False)
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