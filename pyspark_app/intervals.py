from datetime import datetime,timedelta
import ctypes
import time
import logging

from .utils import timezone

logger = logging.getLogger(__name__)


class TimeInterval(object):
    ID = None
    NAME = None

    @classmethod
    def interval_endtime(cls,starttime):
        return starttime + cls.TASK_INTERVAL

    @classmethod
    def next_interval(cls,dt):
        return dt + cls.TASK_INTERVAL

    @classmethod
    def previous_interval(cls,dt):
        return dt - cls.TASK_INTERVAL

    @classmethod
    def interval_starttime(cls,offset=0,starttime=None):
        if starttime:
            return cls._interval_starttime(starttime)
        else:
            now = timezone.localtime()
            #the end time of the interval before the current interval
            endtime = cls._interval_starttime(now)

            #get the end time of the interval after considering the offset
            while (now - endtime).total_seconds() < offset:
                endtime = cls.previous_interval(endtime)

            #return the start time of the interval
            return cls.previous_interval(endtime)

    @classmethod
    def _interval_starttime(cls,t):
        return None

    @classmethod
    def format4filename(cls,t):
        return timezone.format(t,cls.PATTERN4FILENAME)

    @classmethod
    def format(cls,t):
        return timezone.format(t,cls.PATTERN)

    @classmethod
    def intervals(cls,offset=0,starttime=None,endtime=None):
        now = timezone.localtime()
        endtime = endtime or now
        endtime = cls._interval_starttime(endtime)
        istarttime = cls.interval_starttime(offset=offset,starttime=starttime)
        while True:
            iendtime = cls.interval_endtime(istarttime)
            if iendtime <= endtime and (now - iendtime).total_seconds() >= offset:
                yield(istarttime,iendtime)
                istarttime = iendtime
            else:
                return
 
class Monthly(TimeInterval):
    ID = 601
    NAME ="Monthly"
    PATTERN4FILENAME = "%Y%m%d"
    PATTERN = "%Y-%m-%d 00:00:00"

    MAX_OFFSET = 28 * 86400
    
    @classmethod
    def interval_endtime(cls,starttime):
        if starttime.month == 12:
            return starttime.replace(year=starttime.year + 1,month=1)
        else:
            return starttime.replace(month=starttime.month + 1)

    @classmethod
    def _interval_starttime(cls,t):
        t = timezone.localtime(t)
        if t.day != 1 or t.hour != 0 or t.minute != 0 or t.second != 0 or t.microsecond != 0:
            return t.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
        else:
            return t

    @classmethod
    def next_interval(cls,dt):
        if dt.month == 12:
            return dt.replace(year=dt.year + 1,month=1)
        else:
            return dt.replace(month=dt.month + 1)

    @classmethod
    def previous_interval(cls,dt):
        if dt.month == 1:
            return dt.replace(year=dt.year - 1,month=12)
        else:
            return dt.replace(month=dt.month - 1)

class Weekly(TimeInterval):
    TASK_INTERVAL = timedelta(days=7)
    MAX_OFFSET = TASK_INTERVAL.total_seconds()
    PATTERN4FILENAME = "%Y%m%d"
    PATTERN = "%Y-%m-%d 00:00:00"

    @classmethod
    def _interval_starttime(cls,t):
        t = timezone.localtime(t)
        if t.hour != 0 or t.minute != 0 or t.second != 0 or t.microsecond != 0:
            t = t.replace(hour=0,minute=0,second=0,microsecond=0)

        if t.weekday() == cls.week_startday:
            return t
        elif cls.week_startday < t.weekday():
            return t - timedelta(days=t.weekday() - cls.week_startday)
        else:
            return t - timedelta(days=t.weekday() + 7 - cls.week_startday)


class MondayBasedWeekly(Weekly):
    ID = 501
    NAME = "MondayBasedWeekly"

    week_startday = 0

class MondayBasedFourWeekly(MondayBasedWeekly):
    ID = 504
    NAME = "MondayBasedFourWeekly"

    week_startday = 0
    TASK_INTERVAL = timedelta(days=28)
    MAX_OFFSET = TASK_INTERVAL.total_seconds()

class SundayBasedWeekly(Weekly):
    ID = 561
    NAME = "SundayBasedWeekly"

    week_startday = 6

class SundayBasedFourWeekly(SundayBasedWeekly):
    ID = 564
    NAME = "SundayBasedFourWeekly"

    TASK_INTERVAL = timedelta(days=28)
    MAX_OFFSET = TASK_INTERVAL.total_seconds()
    week_startday = 6

class SaturdayBasedWeekly(Weekly):
    ID = 551
    NAME = "SaturdayBasedWeekly"

    week_startday = 5

class SaturdayBasedFourWeekly(SaturdayBasedWeekly):
    ID = 554
    NAME = "SaturdayBasedFourWeekly"

    TASK_INTERVAL = timedelta(days=28)
    MAX_OFFSET = TASK_INTERVAL.total_seconds()
    week_startday = 5

class Daily(TimeInterval):
    ID = 401
    NAME = "Daily"
    PATTERN4FILENAME = "%Y%m%d"
    PATTERN = "%Y-%m-%d 00:00:00"

    TASK_INTERVAL = timedelta(days=1)
    MAX_OFFSET = TASK_INTERVAL.total_seconds()

    @classmethod
    def _interval_starttime(cls,t):
        t = timezone.localtime(t)
        if t.hour != 0 or t.minute != 0 or t.second != 0 or t.microsecond != 0:
            return t.replace(hour=0,minute=0,second=0,microsecond=0)
        else:
            return t

class Hourly(TimeInterval):
    ID = 301
    NAME = "Hourly"
    PATTERN4FILENAME = "%Y%m%d%H"
    PATTERN = "%Y-%m-%d %H:00:00"

    TASK_INTERVAL = timedelta(hours=1)
    MAX_OFFSET = 86400

    @classmethod
    def _interval_starttime(cls,t):
        t = timezone.localtime(t)
        if t.minute != 0 or t.second != 0 or t.microsecond != 0:
            return t.replace(minute=0,second=0,microsecond=0)
        else:
            return t

class Minutely(TimeInterval):
    ID = 201
    NAME = "Minutely" 
    MINUTES = 1
    PATTERN4FILENAME = "%Y%m%d%H%M"
    PATTERN = "%Y-%m-%d %H:%M:00"

    TASK_INTERVAL = timedelta(minutes=1)
    MAX_OFFSET = 86400

    @classmethod
    def interval_endtime(cls,starttime):
        return starttime + cls.TASK_INTERVAL

    @classmethod
    def _interval_starttime(cls,t):
        t = timezone.localtime(t)
        if t.second != 0 or t.microsecond != 0:
            t = t.replace(second=0,microsecond=0)
        d = t.minute  % cls.MINUTES 
        if d > 0:
            return t.replace(minute=t.minute - d)
        else:
            return t


class FiveMinutely(Minutely):
    ID = 205
    NAME = "FiveMinutely"
    MINUTES = 5

    TASK_INTERVAL = timedelta(minutes=5)
    MAX_OFFSET = 86400

class TenMinutely(Minutely):
    ID = 210
    NAME = "TenMinutely"
    MINUTES = 10

    TASK_INTERVAL = timedelta(minutes=10)
    MAX_OFFSET = 86400

class HalfHourly(Minutely):
    ID = 230
    NAME = "HalfHourly"
    MINUTES = 30

    TASK_INTERVAL = timedelta(minutes=30)
    MAX_OFFSET = 86400

intervallist = []
interval_map = {}
def _populate_intervallist(parentcls = TimeInterval):
    for cls in parentcls.__subclasses__():
        if cls.ID and cls.NAME:
            intervallist.append((cls.ID,cls.NAME))
            interval_map[cls.ID] = cls
            interval_map[cls.NAME] = cls
        _populate_intervallist(cls)

_populate_intervallist()

intervallist.sort(key=lambda o:o[0])

def get_interval(idOrName):
    try:
        return interval_map[idOrName]
    except KeyError as ex:
        raise Exception("The interval type({}) not declared.".format(idOrName))

