import json
from datetime import datetime,date,timedelta

from .utils import timezone


class JSONFormater(json.JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """
        
    def default(self, obj):
        if isinstance(obj, datetime):
            if obj.microsecond == 0:
                return timezone.localtime(obj).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return timezone.localtime(obj).strftime("%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(obj, date):
            return obj.strftime(obj,"%Y-%m-%d")
        elif isinstance(obj, timedelta):
            return "{}.{}".format(obj.days * 86400 + obj.seconds,obj.microseconds)
        else:
            return JSONEncoder.default(self, obj)

