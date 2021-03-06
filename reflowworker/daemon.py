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
        self.__stdin = os.devnull
        self.__stdout = os.devnull
        self.__stderr = os.devnull
        self.__pid_file = pid_file

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
            sys.exit("Fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))

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
            sys.exit("Fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.__stdin, 'r')
        so = file(self.__stdout, 'a+')
        se = file(self.__stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pid_file
        atexit.register(self.__delete_pid)
        pid = str(os.getpid())
        file(self.__pid_file, 'w+').write("%s\n" % pid)

    def __delete_pid(self):
        os.remove(self.__pid_file)

    def start(self, debug=False):
        """
        Start the daemon
        """
        # Check for a pid_file to see if the daemon already runs
        try:
            pf = file(self.__pid_file, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            sys.exit(
                "PID file %s already exists. Daemon already running?\n" %
                self.__pid_file
            )

        # Start the daemon
        if not debug:
            self.__daemonize()
        self._run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pid_file
        try:
            pf = file(self.__pid_file, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            sys.exit(
                "PID file %s does not exist. Daemon not running?\n" %
                self.__pid_file
            )

        # Try killing the daemon process
        try:
            while 1:
                pgid = os.getpgid(pid)
                os.killpg(pgid, SIGTERM)
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.__pid_file):
                    os.remove(self.__pid_file)
            else:
                sys.exit(err)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    @abc.abstractmethod
    def _run(self):
        """
        You must override this method when you subclass Daemon.
        It will be called after the process has been daemonized by
        start() or restart().
        """
        return
