import logging
import time
import os
import traceback
import json
import errno
import socket
from datetime import datetime,timedelta

from . import timezone
from .utils import remove_file,file_mtime,set_file_mtime
from .encoder import JSONEncoder,JSONDecoder


logger = logging.getLogger(__name__)

class AlreadyLocked(Exception):
    pass

class InvalidLockStatus(Exception):
    pass

class FileLock(object):
    def __init__(self,lockfile,expired=None,timeout=None):
        """
        params:
        lockfile: the file path of the lock
        expired: lock expire time in seconds
        timeout: the maximum time to acquire the lock; if none, will wait for ever until getting the lock
        """
        self.lockfile = lockfile
        if expired is not None and expired <= 0:
            self.expired = None
        else:
            self.expired = expired

        if not timeout:
            self.timeout = None
        elif timeout < 0 :
            self.timeout = None
        else:
            self.timeout = timedelta(seconds=timeout)

        self.previous_renew_time = None

    def acquire(self):
        """
        Try to acquire the exclusive lock before timeout
        Throw AlreadyLocked exception if can't obtain the lock before timeout
        """
        fd = None
        start_time = timezone.localtime()
        while True:
            try:
                fd = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
                os.write(fd,json.dumps({
                    "host": socket.getfqdn(),
                    "pid":os.getpid(),
                    "lock_time":timezone.localtime()
                },cls=JSONEncoder).encode())
                #lock is acquired
                self.previous_renew_time = file_mtime(self.lockfile)
                break
            except OSError as e:
                if e.errno == errno.EEXIST:
                    #lock is exist, check whether it is expired or not.
                    if self.expired and timezone.localtime() > file_mtime(self.lockfile) + timedelta(seconds=self.expired):
                        #lockfile is expired,remove the lock file
                        remove_file(self.lockfile)
                        continue
                    if not self.timeout or timezone.localtime() - start_time < self.timeout:
                        #wait half second, try again
                        time.sleep(0.5)
                        continue
                    else:
                        #timeout, throw exception
                        metadata = None
                        with open(self.lockfile,"r") as f:
                            metadata = f.read()
                        if metadata:
                            try:
                                metadata = json.loads(metadata,cls=JSONDecoder)
                            except:
                                metadata = {}
                        else:
                            metadata = {}
                        raise AlreadyLocked("Already Locked at {2} and renewed at {3} by process({1}) running in host({0})".format(metadata["host"],metadata["pid"],metadata["lock_time"],file_mtime(self.lockfile)))
                else:
                    raise
            finally:
                if fd:
                    try:
                        os.close(fd)
                    except:
                        pass
    
    def renew(self):
        """
        Acquire the exclusive lock, and return the renew time
        Throw InvalidLockStatus exception if the previous_renew_time is not matched.
        """
        if file_mtime(self.lockfile) != self.previous_renew_time:
            raise InvalidLockStatus("The lock's last renew time({}) is not equal with the provided last renew time({})".format(file_mtime(self.lockfile),self.previous_renew_time))
    
        self.previous_renew_time = set_file_mtime(self.lockfile)
    
    def release(self):
        """
        relase the lock
        """
        remove_file(self.lockfile)
        self.previous_renew_time = None

    @property
    def lock_acquired(self):
        return True if self.previous_renew_time and file_mtime(self.lockfile) != self.previous_renew_time else False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self,t,value,tb):
        self.release()
        self.previous_renew_time = None
        return False if value else True
