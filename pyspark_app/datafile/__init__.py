import logging
import traceback

from . import csv,jsonlistline,jsonline

logger = logging.getLogger(__name__)

filetypes = {}

def initialize():
    for f in [jsonline,csv,jsonlistline]:
        filetypes[f.format] = f

initialize()

def writer(filetype,file=None,filetype_kwargs=None,**kwargs):
    if filetype_kwargs:
        return filetypes[filetype].writer(filetype_kwargs=filetype_kwargs,file=file,**kwargs)
    else:
        return filetypes[filetype].writer(file=file,**kwargs)

def reader(filetype,file,filetype_kwargs=None,header=None,has_header=True):
    """
    """
    if filetype_kwargs:
        return filetypes[filetype].reader(filetype_kwargs=filetype_kwargs,file=file,header=header,has_header=has_header)
    else:
        return filetypes[filetype].reader(file=file,header=header,has_header=has_header)
