import os, shutil, sys, errno
from os import PathLike
from fnmatch import fnmatch
from urllib.request import urlopen
import pandas as pd
from typing import List, Tuple, Union


# TODO: AWAITING REFACTORING 📌

def abs_path(p: Union[str, PathLike], abs: bool=True) -> Union[str, PathLike]:
    return os.path.abspath(p).replace(os.sep, '/') if abs == True else p


def get_path(p: Union[str, PathLike]) -> Tuple[str, str, str]:
    fdir = abs_path(os.path.dirname(p))
    fn, ext = os.path.splitext(os.path.basename(p))
    return fdir, fn, ext


def make_dir(p: Union[str, PathLike]) -> Union[str, PathLike]:
    try:
        if not os.path.exists(p): os.makedirs(p)
    except OSError as e:
        print(f'ERROR: \"{p}\"', e)
    return abs_path(p)


def make_data_dir(p: Union[str, PathLike],
                  data_column: str) -> List[Union[str, PathLike]]:
    df = pd.read_parquet(path=p, engine='pyarrow')
    data_path = df[data_column].values
    data_dir = [make_dir(os.path.dirname(pth)) for pth in data_path]
    return data_dir


def remove_exist(p: Union[str, PathLike]):
    try:
        if os.path.isfile(p) or os.path.islink(p):
            os.remove(p)      # <-- REMOVE THE FILE
        elif os.path.isdir(p):
            shutil.rmtree(p)  # <-- REMOVE DIR AND ALL CONTAINS
    except Exception as e:
        print('ERROR', e)
        pass


def query_files_in_dir(p: Union[str, PathLike], **kwargs) -> List[Union[str, PathLike]]:
    query = kwargs.get('q')
    pat = kwargs.get('pat')
    pattern = '*'+pat+'*' if pat else '*'
    all_ = os.listdir(p)
    dirs = [abs_path(d) for d in all_ if os.path.isdir(d) and fnmatch(d,pattern)]
    files = [abs_path(f) for f in all_ if os.path.isfile(f) and fnmatch(f,pattern)]
    path_all = [abs_path(x) for x in all_ if fnmatch(x,pattern)]
    if query=='dirs': return dirs
    elif query=='files': return files
    else : return path_all


def clear_print_line():
    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')


def chunks_list(li: list, pad: int) -> List[Union[str, int, float]]:
    n = max(1, pad)
    return list(li[i:i+n] for i in range(0, len(li), pad))


def chunks_gen(li, n):  # <-- CHUNKS GENERATOR
    for i in range(0, len(li), n):
        yield li[i:i + n]


def repeat_component(li: list, count: int) -> List[Union[str, int, float]]:
    repeat_list = []
    for c in range( 0, len(li), 1 ):
        for i in range( 0, count, 1 ):
            repeat_list.append(str(li[c]))
    return repeat_list