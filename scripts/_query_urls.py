import os
import errno
from urllib.request import urlopen
from typing import List

from ._timeformats import query_datetime, strip_time
from ._datasort import datasort


def query_host_url() -> str :

    himawari_urls = ['https://ncthmwrwbtst.cr.chiba-u.ac.jp',
                     'https://himawari.asia',
                     'https://himawari8.nict.go.jp']
    for i in range( 0, len(himawari_urls), 1 ):
        response = urlopen(himawari_urls[i], timeout=5).status
        return himawari_urls[i] if response == 200 else print('NO RESPONSE WAS RECEIVED FROM THE URL.')


def date_path(start_time:str, end_time:str, **kwargs: str) -> List[str]:

    timezone = kwargs.get('time')
    utc = query_datetime(start_time, end_time, timezone)
    year = [(strip_time(s, q='year')) for s in utc]
    month = [(strip_time(s, q='month')) for s in utc]
    date = [(strip_time(s, q='date')) for s in utc]
    hour = [(strip_time(s, q='hour')) for s in utc]
    min = [(strip_time(s, q='min')) for s in utc]
    url_post = [(
        year[i] + '/' +
        month[i] + '/' +
        date[i] + '/' +
        hour[i] +
        min[i] +
        '00_' ) for i in range( 0, len(utc), 1 )]
    path_post = [(
        year[i] + '/' +
        month[i] + '_' +
        date[i] + '/' +
        hour[i] +
        min[i] +
        '00_' ) for i in range( 0, len(utc), 1 )]

    query = kwargs.get('q')
    if query == 'url' :
        return url_post
    elif query == 'path' :
        return path_post
    else :
        raise KeyError(
                errno.ENOENT, os.strerror(errno.ENOENT), query)


def level_path(level: int) -> List[str]:

    url_surfix = []
    for x in range( 0, level, 1 ):
        for y in range( 0, level, 1 ):
            url_surfix.append(str(x) + '_' + str(y) + '.png')

    return url_surfix


def query_url(start_time: str, end_time: str, level: int) -> List[str]:

    host_url = query_host_url()
    data_type = 'D531106'  # <-- DATATYPE : HIMAWARI VISIBLE LIGHT
    prefix = f'{host_url}/img/{data_type}/{str(level)}d/550/'
    url_date = date_path(start_time, end_time, time='utc', q='url')
    url_level = level_path(level)
    urls = []
    for i in range( 0, len(url_level), 1 ):
        for j in range( 0, len(url_date), 1 ):
            urls.append(url_date[j] + url_level[i])
    urls = [(prefix+url) for url in urls]
    urls = datasort(urls)

    return urls