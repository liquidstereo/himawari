from os import PathLike
import pandas as pd
from pandas import DataFrame
from typing import Union

from ._common import repeat_component
from ._query_data_path import query_tmplog_path
from ._query_urls import query_datetime, query_url
from ._query_image_path import query_tile_img_path, query_concat_img_path


def create_dataframe(tile_img_dir: Union[str, PathLike],
                     concat_img_dir: Union[str, PathLike],
                     tmplog_dir: Union[str, PathLike],
                     start_time: str,
                     end_time: str,
                     level: int) -> DataFrame:

    print(f'PREPARING DATA FILE. PLEASE WAIT ...', end='\r', flush=True)

    dimention = level * level
    localtime_li = query_datetime(start_time, end_time, 'localtime')
    localtime = repeat_component(localtime_li, dimention)
    utc_li = query_datetime(start_time, end_time, 'utc')
    utc = repeat_component(utc_li, dimention)
    urls = query_url(start_time, end_time, level)
    tile_img_path = query_tile_img_path(tile_img_dir, start_time, end_time, level)
    concat_img_path = query_concat_img_path(concat_img_dir, localtime, level)
    tmplog_file_path = query_tmplog_path(tmplog_dir, start_time, level, urls)
    idx = [i for i in range(0, len(urls), 1)]

    data_dict = {'index': idx,
                 'localtime': localtime,
                 'utc': utc,
                 'level': level,
                 'url': urls,
                 'status': 0,
                 'size': 0,
                 'download': False,
                 'tile_img_path': tile_img_path,
                 'concat_img_path': concat_img_path,
                 'concat': False,
                 'log_path': tmplog_file_path}

    df = pd.DataFrame(data=data_dict)
    df = df.sort_values(by=['url'], ascending=True)

    return df