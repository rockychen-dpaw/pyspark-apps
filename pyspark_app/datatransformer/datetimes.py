import logging

from datetime import datetime,timezone

from .base import *

logger = logging.getLogger(__name__)

_default_tz = None

def get_default_timezone():
    global _default_tz
    if not _default_tz:
        _default_tz = datetime.now(timezone.utc).astimezone().tzinfo

    return _default_tz

def str2unixtime(d, pattern="%Y-%m-%dT%H:%M:%S%z",timezone=None,truncate_to="second"):
    d = datetime.strptime(d,pattern)
    if not d.tzinfo:
        #is a naive datetime
        d = d.replace(tzinfo = timezone or get_default_timezone())
    if not truncate_to or truncate_to == "millisecond":
        return d.timestamp()
    elif truncate_to == "second":
        return int(d.timestamp())
    elif truncate_to == "hour":
        return int(d.replace(minute=0,second=0,microsecond=0).timestamp())
    elif truncate_to == "day":
        return int(d.replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
    elif truncate_to == "month":
        return int(d.replace(day=0,hour=0,minute=0,second=0,microsecond=0).timestamp())
    else:
        raise Exception("The value({}) of the parameter 'truncate_to' is not suported.".format(truncate_to))
        


def str2datetime(d, pattern="%Y-%m-%dT%H:%M:%S%z",srctimezone=None,targettimezone=None):
    d = datetime.strptime(d,pattern)
    if not d.tzinfo:
        #is a naive datetime
        d = d.replace(tzinfo = timezone or get_default_timezone())

    return d.astimezone(targettimezone or get_default_timezone())

def clean():
    pass

transformers = [str2unixtime,str2datetime]
