import logging
import traceback

from . import csv,jsonlistline,jsonline

logger = logging.getLogger(__name__)

filetypes = {}

def initialize():
    for f in [jsonline,csv,jsonlistline]:
        filetypes[f.format] = f

initialize()

def writer(filetype,file=None,**kwargs):
    return filetypes[filetype].writer(file=file,**kwargs)

def reader(filetype,file,headers=None,has_header=True):
    """
    """
    return filetypes[filetype].reader(file=file,headers=headers,has_header=has_header)
