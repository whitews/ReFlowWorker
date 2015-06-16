import multiprocessing
from logger import logger
from processing_error import ProcessingError

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

        logger.info(
            "ProcessRequest %s assigned to GPU %d",
            str(assigned_pr_id),
            self.device
        )

        try:
            pr_response = utils.get_process_request(
                self.host,
                self.token,
                assigned_pr_id,
                method=self.method
            )

            self.assigned_pr = ProcessRequest(
                self.host,
                self.token,
                pr_response['data'],
                self.method
            )
        except ProcessingError as e:
            # any ProcessingError should have already been logged,
            # so just report it back to the ReFlow server.
            # Also, re-raise a ProcessingError so the Worker can avoid
            # running this analysis
            self.report_errors(e.message)
            raise ProcessingError("Fatal error creating WorkerProcess")
        except Exception, e:
            logger.error(str(e))
            self.report_errors(
                "Unknown error occurred parsing ProcessRequest from server"
            )
            raise ProcessingError("Fatal error creating WorkerProcess")

    def run(self):
        # We've got something to do!
        try:
            self.assigned_pr.analyze(self.device)
        except ProcessingError as e:
            # any ProcessingError should have already been logged,
            # so just report it back to the ReFlow server
            self.report_errors(e.message)
            return
        except Exception as e:
            logger.error(str(e))
            self.report_errors("Unknown error occurred during processing")
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
                # we're not assigned anymore, no need to report errors
                # just return
                return
        except Exception as e:
            logger.error(str(e))
            self.report_errors(
                "Unknown error occurred verifying assignment after processing"
            )
            return

        # Upload results
        try:
            self.assigned_pr.post_clusters()
        except ProcessingError as e:
            # any ProcessingError should have already been logged,
            # so just report it back to the ReFlow server
            self.report_errors(e.message)
            return
        except Exception, e:
            logger.error(str(e))
            self.report_errors(
                "Unknown error occurred saving cluster results to server"
            )
            return

        # Report the ProcessRequest is complete
        try:
            utils.complete_pr_assignment(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                method=self.method
            )
        except Exception as e:
            # This would be an odd scenario! If the PR was legitimately
            # assigned to this worker and the server was still running,
            # then there's not much we can do. We will still attempt to
            # report an error, but it's doubtful that will work either.
            logger.error(str(e))
            self.report_errors(
                "Unknown error occurred attempting to mark request 'Complete'"
            )
            return

        logger.info(
            "(PR: %s) ProcessRequest marked as complete",
            str(self.assigned_pr.process_request_id)
        )

        # TODO: Clean up! Delete the local files

    def report_errors(self, message):
        """
        Report an error back to the ReFlow server. This will update the
        PR status to 'Error' and the PR will no longer be viable.
        """
        try:
            utils.report_pr_error(
                self.host,
                self.token,
                self.assigned_pr.process_request_id,
                message,
                method=self.method
            )
        except Exception as e:
            # Not much we can do except log the error locally to trouble-shoot
            logger.error(str(e))
