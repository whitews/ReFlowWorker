# Modified from Sander Marechal' Public Domain code here:
#    http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/

import sys
import os
import time
import atexit
import abc
from signal import SIGTERM


class Daemon(object):
    """
    Abstract base class for creating a daemon.

    Usage: subclass the Daemon class and override the run() method
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, pid_file):
        self.stdin = os.devnull
        self.stdout = os.devnull
        self.stderr = os.devnull
        self.pid_file = pid_file

    def __daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write(
                "Fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write(
                "Fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pid_file
        atexit.register(self.delete_pid)
        pid = str(os.getpid())
        file(self.pid_file, 'w+').write("%s\n" % pid)

    def delete_pid(self):
        os.remove(self.pid_file)

    def start(self, debug=False):
        """
        Start the daemon
        """
        # Check for a pid_file to see if the daemon already runs
        try:
            pf = file(self.pid_file, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "PID file %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pid_file)
            sys.exit(1)

        # Start the daemon
        if not debug:
            self.__daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pid_file
        try:
            pf = file(self.pid_file, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "PID file %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pid_file)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pid_file):
                    os.remove(self.pid_file)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    @abc.abstractmethod
    def run(self):
        """
        You must override this method when you subclass Daemon.
        It will be called after the process has been daemonized by
        start() or restart().
        """
        return