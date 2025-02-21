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
from .. import database
from .enums import get_enum,get_enum_key
from .helper import transformer_factory

logger = logging.getLogger(__name__)

_transformers = {}
_declared_transformers = {}

for func in itertools.chain(datetimes.transformers,enums.transformers,adb2c.transformers):
    _transformers[func.__name__] = transformer_factory(func)

def clean():
    datetimes.clean()
    enums.clean()

def transform(f_name,val,databaseurl=None,columnid=None,context=None,record=None,columnname=None,return_id=True,**kwargs):
    try:
        if f_name == "datatransform" :
            transformer = kwargs.pop("transformer")
            try:
                _func = _declared_transformers[transformer]
            except KeyError as ex:
                _func = transformer_factory(datatransformer.get_transformer(databaseurl,transformer))
                _declared_transformers[transformer] = _func
            return _func[0](_func[1],val,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs)
        else:
            _func,f =  _transformers[f_name]
            return _func(f,val,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs)
    except Exception as ex:
        #failed ,write the message, and raise the exception
        sql = """
INSERT INTO datascience_runningissue
("phase","category","dstime","dsfile","message","created") 
VALUES 
('{0}','{1}', '{2}','{3}','{4}','{5}')
""".format(
            context["phase"] if context else "Unknown",
            context['category'] if context else "Unknown", 
            timezone.dbtime(context.get("dstime")) if context and context.get("dstime") else 'Unknown', 
            context.get("dsfile","Unknown") if context else "Unknown",
            "Failed to tranform the recode({}).msg={}".format(record,str(ex)),
            timezone.dbtime()
        )
        with database.Database(databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    conn.commit()
                except:
                    conn.rollback()
                    raise
        raise 

def is_enum_func(f_name): 
    return f_name in ["str2enum","number2group","str2group","domain2enum","ip2city","ip2country","resourcekey"]

def is_group_func(f_name): 
    return f_name in ["number2group","str2group"]


atexit.register(clean)
