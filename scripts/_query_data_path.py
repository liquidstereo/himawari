import os
from os import PathLike
from typing import List, Tuple, Union

from ._common import abs_path
from ._datasort import datasort


def query_dir_path(dir_path: Union[str, PathLike],
                   start_time: str,
                   level: int) -> Tuple[str, str]:

    time_year = start_time.split('-')[0]
    time_date = start_time.split(' ')[0].replace('-', '_')
    dir_level = f'level_{str(level).zfill(2)}'
    dir_root = abs_path(os.path.join(dir_path, dir_level, time_year))
    return dir_root, time_date


def query_datafile_path(data_root: Union[str, PathLike],
                        start_time: str,
                        level: int) -> Union[str, PathLike]:

    data_dir, time_date = query_dir_path(data_root, start_time, level)
    os.makedirs(data_dir, exist_ok=True)
    datafile_path = abs_path(os.path.join(data_dir, time_date + '.gz'))
    return datafile_path


def query_tmplog_path(tmplog_root: Union[str, PathLike],
                      start_time: str,
                      level: int,
                      urls: list) -> List[Union[str, PathLike]]:

    tmplog_dir = query_dir_path(tmplog_root, start_time, level)[0]
    tmplog_file_name = [(url.split('/550/')[1].replace('.png', '.log')) for url in urls]
    tmplog_file_path = [abs_path(os.path.join(tmplog_dir, f)) for f in tmplog_file_name]
    tmplog_file_path = datasort(tmplog_file_path)
    return tmplog_file_path


def query_errorfile_path(datafile_path: Union[str, PathLike]) -> Union[str, PathLike]:
    error_filepath = datafile_path.replace('data', 'error')
    os.makedirs(os.path.dirname(error_filepath), exist_ok=True)
    return error_filepath


def query_tmpfile_path(datafile_path: Union[str, PathLike]) -> Union[str, PathLike]:
    tmp_filepath = datafile_path.replace('data', 'tmp')
    os.makedirs(os.path.dirname(tmp_filepath), exist_ok=True)
    return tmp_filepath