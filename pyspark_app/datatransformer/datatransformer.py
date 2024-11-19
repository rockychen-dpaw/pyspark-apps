import logging
from ..utils.dynamicfunction import DynamicFunction
from .. import database

logger = logging.getLogger(__name__)

class DataTransformer(DynamicFunction):
    f_name = ("transform",1)
    modules = {}
    functions = {}

    def load(self):
        with database.Database(self.databaseurl).get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("select code,modified from report_datatransformer where name = '{}'".format(self.name))
                data = cursor.fetchone()
                if not data:
                    raise Exception("Datatransfer({}) does not exist".format(self.name))
                return data

_transformers = {}
_optional_params = ("databaseurl","columnid","context","record","columnname","return_id")
transfer_method_pattern1 = "lambda f,val,{0},**kwargs:f(val,{1},**kwargs)"
transfer_method_pattern2 = "lambda f,val,{0},**kwargs:f(val,**kwargs)"
def datatransform(data,databaseurl=None,columnid=None,context=None,record=None,columnname=None,return_id=None,transformer=None,**kwargs):
    if not transformer:
        raise Exception("Transformer is missing.")

    transformerobj = DataTransformer(databaseurl,transformer)
    metadata,cached = transformerobj.function_metadata
    if transformer not in _transformers or not cached:
        if metadata[2] and any(p for p in _optional_params if p in metadata[2]):
            _func =  eval(transfer_method_pattern1.format((",".join("{}=None".format(p) for p in _optional_params )),(",".join("{0}={0}".format(p) for p in _optional_params if p in metadata[2]))))
        else:
            _func =  eval(transfer_method_pattern2.format((",".join("{}=None".format(p) for p in _optional_params )) ))
        _transformers[transformer] = _func
    else:
        _func = _transformers[transformer]

    return _func(metadata[1],data,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs)

transformers = [datatransform]
