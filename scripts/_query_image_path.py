import os
from os import PathLike
from typing import List, Union

from ._common import abs_path
from ._timeformats import strip_time
from ._query_urls import date_path, level_path
from ._datasort import datasort


def query_tile_img_path(tile_img_dir: Union[str, PathLike],
                        start_time: str,
                        end_time: str,
                        level: int) -> List[Union[str, PathLike]]:

    tile_dir = tile_img_dir + '/level_'+str(level).zfill(2)
    tile_img_path = []
    tile_img_date = date_path(start_time, end_time, time='localtime', q='path')
    tile_img_level = level_path(level)
    for y in range( 0, len(tile_img_level), 1 ):
        for x in range( 0, len(tile_img_date), 1 ):
            tile_img_path.append(tile_img_date[x]+tile_img_level[y])

    tile_img_path = [abs_path(os.path.join(tile_dir, p)) for p in tile_img_path]
    tile_img_path = datasort(tile_img_path)

    return tile_img_path


def query_concat_img_path(concat_img_dir: Union[str, PathLike],
                          local_times: list,
                          level: int) -> List[Union[str, PathLike]]:

    concat_dir = concat_img_dir + '/level_'+str(level).zfill(2)
    concat_img_path = []
    for t in local_times :
        year = strip_time(t, q='year')
        month = strip_time(t, q='month')
        date = strip_time(t, q='date')
        hour = strip_time(t, q='hour')
        minute = strip_time(t, q='min')
        concat_img_path.append(year+'_'+month+'_'+date+'_'+hour+'_'+minute+'.png')

    concat_img_path = [abs_path(os.path.join(concat_dir, f)) for f in concat_img_path]
    concat_img_path = datasort(concat_img_path)

    return concat_img_path