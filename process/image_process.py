import os, sys
from os import PathLike
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pandas import DataFrame
from typing import Any, Callable, List, Optional, Sequence, Tuple, Type, Union
from colorama import Fore, Back, Style
from colorama import init
init(autoreset=True)

sys.path.append('../')
from scripts import list_to_array, concatenate_images_multiple_threads
from scripts import datasort
from scripts import update_tmpfile


def query_confirm_download(df: DataFrame) -> DataFrame:

    download_df = df.copy()
    download_files = download_df['tile_img_path'].to_list()
    download_values = download_df['download'].to_list()

    for i in range( 0, len(download_files), 1 ):
        if os.path.isfile(download_files[i]) and download_values[i] is False:
            filesize = os.path.getsize(download_files[i])
            download_df.at[i, 'download'] = True
            download_df.at[i, 'status'] = 200
            download_df.at[i, 'size'] = filesize
        elif not os.path.isfile(download_files[i]) :
            download_df.at[i, 'download'] = False
        else :
            pass

    return download_df


def query_data_to_concat(df: pd.DataFrame) -> pd.DataFrame:

    confirm_download_df = query_confirm_download(df)
    concat_df = confirm_download_df.copy()
    undownload = concat_df[concat_df['download'] == False].index.to_list()
    concat_df.drop(index=undownload, inplace=True)
    if len(concat_df.index) == 0 :
        print(f'{Fore.RED}NO DATA DOWNLOADED{Fore.RESET}')

    return concat_df[concat_df['concat'] == False]


def sort_data_paths(df: pd.DataFrame) -> List[dict[str|PathLike, str|PathLike]]:

    tile_img_path = df['tile_img_path'].astype(dtype='object', errors='ignore').to_list()
    concat_img_path = df['concat_img_path'].astype(dtype='object', errors='ignore').to_list()

    tile_imgs = [(path) for path in tile_img_path]
    concat_imgs = list(set(concat_img_path))
    tile_imgs = datasort(tile_imgs)
    concat_imgs = datasort(concat_imgs)
    tile_imgs_arr = list_to_array(tile_imgs, len(concat_imgs))

    sorted_data_list = []
    for i in range( 0, len(tile_imgs_arr), 1 ):
        data_sorted = {'concat_imgs': concat_imgs[i],
                       'tile_imgs': tile_imgs_arr[i]}
        sorted_data_list.append(data_sorted)  # <-- DICT DATA LIST

    return sorted_data_list


def update_dataframe_by_result(df: pd.DataFrame) -> pd.DataFrame:
    concat_imgs = df['concat_img_path'].to_list()
    concat_result = [True if os.path.isfile(f) else False for f in concat_imgs]
    for i in range( 0, len(concat_imgs), 1 ):
        df.at[i, 'concat'] = concat_result[i]
    return df


def update_result_into_tmpfile(datafile: Union[str, PathLike],
                               column: str) -> Union[str, PathLike]:
    tmpfile = datafile.replace('data', 'tmp')
    if os.path.isfile(tmpfile) :
        update_tmpfile(datafile, tmpfile, column)


def concatenate_tiled_images(datafile: Union[str, PathLike]) -> Union[str, PathLike]:

    df = pd.read_parquet(path=datafile, engine='pyarrow')
    df = df.sort_values(by=['url'], ascending=True)
    level = int(df['level'][0])

    df_to_concat = query_data_to_concat(df)
    if len(df_to_concat.index) == 0 :
        print(f'{Fore.RED}NO DATA TO CONCATENATE.{Fore.RESET}')
        return datafile

    else :
        sorted_data_path = sort_data_paths(df_to_concat)
        concat_list = [(p['concat_imgs']) for p in sorted_data_path]
        tile_list = [(p['tile_imgs']) for p in sorted_data_path]

        concatenate_images_multiple_threads(concat_list, tile_list, level)

        df = update_dataframe_by_result(df)
        pa_table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(pa_table, datafile, compression='gzip')

        update_result_into_tmpfile(datafile, 'concat')

        return datafile


def main():
    concatenate_tiled_images(r'_FILE_PATH_')

if __name__ == '__main__':
    main()