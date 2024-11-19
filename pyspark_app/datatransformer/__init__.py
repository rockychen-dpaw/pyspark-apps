import itertools
import logging
import atexit

from .. import settings
from .base import *
from .. import utils

from . import datetimes 
from . import enums 
from . import adb2c
from . import datatransformer
from .enums import get_enum,get_enum_key

logger = logging.getLogger(__name__)

_transformers = {}
_optional_params = ("databaseurl","columnid","context","record","columnname","return_id")
transfer_method_pattern1 = "lambda f,val,{0},**kwargs:f(val,{1},**kwargs)"
transfer_method_pattern2 = "lambda f,val,{0},**kwargs:f(val,**kwargs)"
def _transform_factory(f):
    kwargs = utils.get_kwargs(f,1)[0]
    if kwargs and any(p for p in _optional_params if p in kwargs):
        _func =  eval(transfer_method_pattern1.format((",".join("{}=None".format(p) for p in _optional_params )),(",".join("{0}={0}".format(p) for p in _optional_params if p in kwargs))))
    else:
        _func =  eval(transfer_method_pattern2.format((",".join("{}=None".format(p) for p in _optional_params )) ))

    return (_func,f)

for func in itertools.chain(datetimes.transformers,enums.transformers,adb2c.transformers,datatransformer.transformers):
    _transformers[func.__name__] = _transform_factory(func)

def clean():
    datetimes.clean()
    enums.clean()

def transform(f_name,val,databaseurl=None,columnid=None,context=None,record=None,columnname=None,return_id=True,**kwargs):
    _func,f = _transformers[f_name]
    return _func(f,val,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs)

def is_enum_func(f_name): 
    return f_name in ["str2enum","number2group","str2group","domain2enum","ip2city","ip2country","resourcekey"]

def is_group_func(f_name): 
    return f_name in ["number2group","str2group"]


atexit.register(clean)
