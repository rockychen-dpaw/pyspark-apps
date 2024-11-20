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

def get_transformer(databaseurl,transformer):
    transformerobj = DataTransformer(databaseurl,transformer)
    return transformerobj.function

