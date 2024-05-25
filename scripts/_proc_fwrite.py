import os
from os import PathLike
import pandas as pd
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Union
from colorama import Fore
from colorama import init
init(autoreset=True)

from ._common import clear_print_line
from ._query_data_path import query_tmpfile_path


# TODO: REFACTORING 📌

def unique_into_tmpfile(unique_df: DataFrame,
                        tmp_filepath: Union[str, PathLike]) -> Union[str, PathLike]:

    table_from_pd = pa.Table.from_pandas(unique_df, preserve_index=False)
    pq.write_table(table_from_pd, tmp_filepath, compression='gzip')
    return tmp_filepath


def append_unique_data(exist_df: DataFrame,
                       unique_df: DataFrame) -> DataFrame:

    df = pd.concat([exist_df, unique_df]).sort_values(by=['url'], ascending=True)
    df_index = [idx for idx in range( 0, len(df['index'].values), 1 )]
    df.drop('index', axis=1, inplace=True)
    df.insert(0, 'index', df_index, allow_duplicates=False)
    return df


def append_exist_dataframe(existfile: Union[str, PathLike],
                           new_df: DataFrame) -> DataFrame:

    exist_df = pd.read_parquet(path=existfile, engine='pyarrow')
    unique_df = pd.concat([new_df, exist_df])
    unique_df = unique_df.drop_duplicates(subset=['url'], keep=False).reset_index(drop=True)

    if len(unique_df.index) == 0:
        clear_print_line()
        print(f'{Fore.RED}NO DATA TO APPENDING.{Fore.RESET}')
        return exist_df

    else :
        unique_into_tmpfile(unique_df, query_tmpfile_path(existfile))  # <-- SAVE UNIQUE AS TMPFILE
        df = append_unique_data(exist_df, unique_df)
        print(f'{Fore.YELLOW}DATA APPENDING ... DONE.{Fore.RESET}', end='\r', flush=True)
        return df


def update_tmpfile(datafile: Union[str, PathLike],
                   tmpfile: Union[str, PathLike],
                   column: str) -> Union[str, PathLike]:

    df_datafile = pd.read_parquet(path=datafile, engine='pyarrow')
    df_tmpfile = pd.read_parquet(path=tmpfile, engine='pyarrow')

    df_datafile_url = df_datafile['url'].tolist()
    df_tmpfile_url = df_tmpfile['url'].tolist()

    data_idx = []
    tmp_idx = []
    for i in range( 0, len(df_datafile_url), 1 ):
        for j in range( 0, len(df_tmpfile_url), 1 ):
            if df_datafile_url[i] == df_tmpfile_url[j] :
                tmp_idx.append(j)
                data_idx.append(i)
    for i in range( 0, len(tmp_idx), 1 ):
        df_tmpfile.at[tmp_idx[i], column] = df_datafile.at[data_idx[i], column]

    pa_table = pa.Table.from_pandas(df_tmpfile, preserve_index=False)
    pq.write_table(pa_table, tmpfile, compression='gzip')


def write_tmplog(tmplog_path: Union[str, PathLike],
                 tmplog_data: dict[str, str]) -> Union[str, PathLike]:

    df = pd.DataFrame.from_dict([tmplog_data])
    os.makedirs(os.path.dirname(tmplog_path), exist_ok=True)
    df.to_csv(tmplog_path, sep=',', mode='w', index=False,    # <-- SAVE AS CSV FILES
              encoding='utf-8-sig', compression='gzip',
              lineterminator='\n')

    return tmplog_path