from .. import settings
from datetime import datetime,timezone

def get_current_timezone():
    return settings.TIMEZONE

def is_aware(dt):
     return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None

def is_naive(dt):
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None

def localtime(dt=None,timezone=None):
    if timezone is None:
        timezone = get_current_timezone()
    
    # If `dt` is naive, astimezone() will raise a ValueError,
    # so we don't need to perform a redundant check.
    if not dt:
        return datetime.now(tz=timezone)
    elif dt.tzinfo == timezone:
        return dt

    dt = dt.astimezone(timezone)
    if hasattr(timezone, 'normalize'):
        # This method is available for pytz time zones.
        dt = timezone.normalize(dt)
    return dt

def parse(dt,pattern="%Y-%m-%d %H:%M:%S",timezone=None):
    return make_aware(datetime.strptime(dt,pattern),timezone=timezone)

def dbtime(dt=None):
    return localtime(dt).strftime("%Y-%m-%d %H:%M:%S%z")

def format(dt=None,pattern="%Y-%m-%d %H:%M:%S"):
    return localtime(dt).strftime(pattern)

def timestamp(dt=None):
    return  localtime(dt,timezone=timezone.utc).timestamp()

def make_aware(dt, timezone=None):
    if timezone is None:
        timezone = get_current_timezone()
    if hasattr(timezone, 'localize'):
        # This method is available for pytz time zones.
        return timezone.localize(dt, is_dst=None)
    else:
        # Check that we won't overwrite the timezone of an aware datetime.
        if is_aware(dt):
            raise ValueError(
                "make_aware expects a naive datetime, got %s" % dt)
        # This may be wrong around DST changes!
        return dt.replace(tzinfo=timezone)




