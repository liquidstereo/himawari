import os, errno
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List
import time


def get_timedelta() -> int:  # <-- GET A TIME ZONE HOUR DIFFERENCES

    timedelta_sec = time.altzone if time.daylight else time.timezone
    timedelta_hour = round(timedelta_sec / 3600)
    return timedelta_hour


def query_datetime(start_time: str, end_time: str, q: str) -> List[str]:

    interval = '10min'  # <-- THE TIME INTERVAL IS AVAILABLE IN 10 MINUTE
    localtimes = pd.date_range(start=pd.Timestamp(start_time).round(interval),
                               end=pd.Timestamp(end_time).round(interval),
                               freq=interval)

    sys_timedelta = get_timedelta()
    utc_timedelta = str(sys_timedelta) + 'hour'
    utc_times = localtimes + pd.Timedelta(utc_timedelta)

    localtime_list = localtimes.strftime('%Y-%m-%d %H:%M:%S').tolist()
    utc_list = utc_times.strftime('%Y-%m-%d %H:%M:%S').tolist()

    if q == 'localtime' :
        return localtime_list
    elif q == 'utc' :
        return utc_list
    else :
        raise ValueError(
            errno.ENOENT, os.strerror(errno.ENOENT), q)


def strip_time(timestamp: datetime, q: str=None) -> str:

    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    if q == 'year':
        return dt.strftime('%Y')
    elif q == 'month':
        return dt.strftime('%m')
    elif q == 'date':
        return dt.strftime('%d')
    elif q == 'hour':
        return dt.strftime('%H')
    elif q == 'min':
        return dt.strftime('%M')
    else :
        raise KeyError(
            errno.ENOENT, os.strerror(errno.ENOENT), q)