import itertools
import logging
import atexit

from .. import settings
from .base import *
from .. import utils

from . import datetimes 
from . import enums 
from .enums import get_enum,get_enum_key

logger = logging.getLogger(__name__)

_transformers = {}
_optional_params = ("databaseurl","columnid","context","record","columnname")
transfer_method_pattern = "lambda f,val,{0},**kwargs:f(val,{1},**kwargs)"
def _transform_factory(f):
    kwargs = utils.get_kwargs(f,1)
    if any(p for p in _optional_params if p in kwargs):
        _func =  eval(transfer_method_pattern.format((",".join("{}=None".format(p) for p in _optional_params )),(",".join("{0}={0}".format(p) for p in _optional_params if p in kwargs))))
    else:
        _func = "lambda f,val,**kwargs:f(val,**kwargs)"

    return (_func,f)

for func in itertools.chain(datetimes.transformers,enums.transformers):
    _transformers[func.__name__] = _transform_factory(func)

def clean():
    datetimes.clean()
    enums.clean()

def transform(f_name,val,databaseurl=None,columnid=None,context=None,record=None,columnname=None,**kwargs):
    _func,f = _transformers[f_name]
    return _func(f,val,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,**kwargs)

def is_enum_func(f_name): 
    return f_name in ["str2enum","number2group","str2group","domain2enum","ip2city","ip2country"]

def is_group_func(f_name): 
    return f_name in ["number2group","str2group"]


atexit.register(clean)
