import multiprocessing
from logger import logger

from reflowrestclient import utils

from process_request import ProcessRequest


class WorkerProcess(multiprocessing.Process):
    def __init__(self, host, token, method, assigned_pr_id, gpu_id):
        super(WorkerProcess, self).__init__()
        self.daemon = True
        self.host = host
        self.token = token
        self.method = method
        self.device = gpu_id

        pr_response = utils.get_process_request(
            self.host,
            self.token,
            assigned_pr_id,
            method=self.method
        )

        logger.info("Starting ProcessRequest %s" % str(assigned_pr_id))

        self.assigned_pr = ProcessRequest(
            self.host,
            self.token,
            pr_response['data'],
            self.method
        )
 
    def run(self):
        # We've got something to do!
        try:
            self.assigned_pr.analyze(self.device)
        except Exception, e:
            logger.error(str(e))
            self.report_errors()
            return

        # Verify assignment
        try:
            verify_assignment_response = utils.verify_pr_assignment(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                method=self.method
            )
            if not verify_assignment_response['data']['assignment']:
                # we're not assigned anymore, return
                return
        except Exception as e:
            logger.error(str(e))
            return

        # Upload results
        try:
            self.assigned_pr.post_clusters()
        except Exception as e:
            logger.error(str(e))
            return

        # Report the ProcessRequest is complete
        try:
            verify_complete_response = utils.complete_pr_assignment(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                method=self.method
            )
            if verify_complete_response['status'] != 200:
                # something went wrong
                raise Exception("Server rejected our 'Complete' request")
        except Exception as e:
            logger.error(str(e))
            return

        # Verify 'Complete' status
        try:
            r = utils.get_process_request(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                method=self.method
            )
            if 'data' not in r:
                raise Exception("Improper host response, no 'data' key")
            if 'status' not in r['data']:
                raise Exception("Improper host response, no 'status' key")
            if r['data']['status'] != 'Complete':
                raise Exception("Failed to mark assignment complete")
        except Exception as e:
            # TODO: should probably do more than just log an error
            # locally, perhaps try to send errors again? then re-try to
            # send complete status again?
            logger.error(str(e))
            return

        # TODO: Clean up! Delete the local files

    def report_errors(self):
        """
        It will be called after process() if that method returned False
        """
        print self
        return
