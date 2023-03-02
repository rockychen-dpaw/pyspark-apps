import datetime
import json
import logging

from .. import settings
from . import timezone

logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    """
    A JSON encoder to support encode datetime
    """
    def default(self,obj):
        if isinstance(obj,datetime.datetime):
            return {
                "_type":"datetime",
                "value":timezone.localtime(obj).strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
        elif isinstance(obj,datetime.date):
            return {
                "_type":"date",
                "value":obj.strftime("%Y-%m-%d")
            }
        else:
            return json.JSONEncoder.default(self,obj)

class JSONDecoder(json.JSONDecoder):
    """
    A JSON decoder to support decode datetime
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj
        t = obj['_type']
        if t == 'datetime':
            return timezone.parse(obj["value"],"%Y-%m-%d %H:%M:%S.%f")
        elif t == 'date':
            return datetime.datetime.strptime(obj["value"],"%Y-%m-%d").date()
        else:
            return obj

