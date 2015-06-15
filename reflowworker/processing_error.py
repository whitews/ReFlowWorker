class ProcessingError(Exception):
    """
    Provides more user-friendly error message to the ReFlow server. In many
    cases ProcessingError acts as a translation for the actual error, and
    the original error/exception is logged and a more useful message is
    given.
    """
    def __init__(self, message):
        self.message = message
        super(ProcessingError, self).__init__(message)
