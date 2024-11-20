import logging
from ..utils.dynamicfunction import DynamicFunction
from .. import database
from .helper import transformer_factory

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
def datatransform(data,databaseurl=None,columnid=None,context=None,record=None,columnname=None,return_id=None,transformer=None,**kwargs):
    if not transformer:
        raise Exception("Transformer is missing.")

    transformerobj = DataTransformer(databaseurl,transformer)
    metadata,cached = transformerobj.function_metadata
    if transformer not in _transformers or not cached:
        _transformers[transformer] = transformer_factory(metadata[1])

    _func,f = _transformers[transformer]

    return _func(f,data,databaseurl=databaseurl,columnid=columnid,context=context,record=record,columnname=columnname,return_id=return_id,**kwargs)

transformers = [datatransform]
