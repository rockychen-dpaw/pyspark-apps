import subprocess
import traceback
import logging
import inspect
import psutil
import shutil
import  os
import csv
import socket
from datetime import datetime

from . import timezone

logger = logging.getLogger("pyspark_app.app.nginxaccesslog")

def get_line_counter(f):
    size = 0
    with open(f) as h:
        for row in csv.reader(h):
            size += 1
        
        return size
    """
    result = subprocess.run(["wc", "-l",f],text=True,shell=False,stdout=subprocess.PIPE) 
    result.check_returncode()
    return int(result.stdout.split()[0])
    """

def filter_file_with_linenumbers(src_file,linenumber_file,target_file):
    #awk 'NR==FNR{ pos[$1]; next }FNR in pos' indexes.txt 2022020811.nginx.access.csv
    result = subprocess.run("awk 'NR==FNR{{ pos[$1]; next }}FNR in pos' '{}' '{}' > '{}'".format(linenumber_file,src_file,target_file),text=True,shell=True,stdout=subprocess.PIPE) 
    result.check_returncode()

def concat_files(files,target_file):
    result = subprocess.run("cat {} > '{}'".format(" ".join( "'{}'".format(f) for f in files),target_file),text=True,shell=True,stdout=subprocess.PIPE) 
    result.check_returncode()

_processid = None
def get_processid():
    global _processid
    if not _processid:
        _processid = "{}-{}-{}".format(socket.gethostname(),os.getpid(),get_process_starttime())
    return _processid

_process_starttime = None
def get_process_starttime():
    global _process_starttime
    if not _process_starttime:
        _process_starttime = timezone.make_aware(datetime.fromtimestamp(psutil.Process(os.getpid()).create_time())).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return _process_starttime


def get_kwargs(f_func,required_parameters):
    """
    Return the optional keyword parameters
    """
    argspec = inspect.getfullargspec(f_func)
    if argspec.varargs or argspec.varkw or argspec.kwonlyargs or ((len(argspec.args) if argspec.args else 0) - required_parameters) != (len(argspec.defaults) if argspec.defaults else 0):
        raise Exception("Function should only have {} required parameters and optional multiple keyword parameters.".format(required_parameters))

    return argspec.args[required_parameters:] if argspec.defaults else []

def remove_file(f):
    if not f: 
        return

    try:
        os.remove(f)
    except:
        logger.error("Failed to remove file({}).{}".format(f,traceback.format_exc()))
        pass

def remove_dir(d):
    if not d: 
        return

    try:
        shutil.rmtree(d)
    except:
        logger.error("Failed to remove the folder({}).{}".format(d,traceback.format_exc()))
        pass



def file_mtime(f):
    return timezone.localtime(datetime.fromtimestamp(os.path.getmtime(f)))

def set_file_mtime(f,d=None):
    """
    setting mtime will also set atime to the same time as mtime
    return the new mtime
    """
    d = timezone.localtime(d)

    t = d.timestamp()

    os.utime(f,times=(t,t))
    return file_mtime(f)


def mkdir(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            if os.path.exists(path):
                #already exist
                return
            else:
                #failed
                raise


    
