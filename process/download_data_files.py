import os, sys, errno
from os import PathLike
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pandas import DataFrame
from typing import Union
from colorama import Fore
from colorama import init
init(autoreset=True)

sys.path.append('../')
from scripts import download_files_threads
from scripts import update_tmpfile


def query_data_to_download(df: DataFrame) -> DataFrame:

    download_df = df.copy()
    download_done = download_df[download_df['download'] == True].index.to_list()
    download_df.drop(index=download_done, inplace=True)

    return download_df[download_df['status'] != 403]


def update_datafile_by_tmplog(datafile: Union[str, PathLike]) -> Union[str, PathLike]:

    df = pd.read_parquet(path=datafile, engine='pyarrow')
    tmplog_files = df['log_path'].to_list()
    tmplog_df = pd.concat(pd.read_csv(f, compression='gzip') for f in tmplog_files if os.path.isfile(f))

    log_size = tmplog_df['size'].astype(dtype='int64', errors='ignore').to_list()
    log_status = tmplog_df['status'].astype(dtype='int64', errors='ignore').to_list()
    log_download = tmplog_df['download'].astype(dtype='bool', errors='ignore').to_list()
    exist_df_url = df['url'].astype(dtype='object', errors='ignore').to_list()
    tmplog_url = tmplog_df['url'].astype(dtype='object', errors='ignore').to_list()

    for i in range( 0, len(exist_df_url), 1 ):
        for j in range( 0, len(tmplog_url), 1 ):
            if tmplog_url[j] == exist_df_url[i] :
                df.at[i,'download'] = log_download[j]
                df.at[i,'size'] = log_size[j]
                df.at[i,'status'] = log_status[j]
            else :
                pass

    pa_table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(pa_table, datafile, compression='gzip')

    return datafile


def update_result_into_tmpfile(datafile: Union[str, PathLike], column: str) -> None:

    tmpfile = datafile.replace('data', 'tmp')
    if os.path.isfile(tmpfile) :
        update_tmpfile(datafile, tmpfile, column)


def download_data_files(datafile: Union[str, PathLike]) -> Union[str, PathLike]:

    df = pd.read_parquet(path=datafile, engine='pyarrow')
    df = query_data_to_download(df)

    if len(df.index) == 0 :
        print(f'{Fore.RED}NO DATA TO DONWLOAD.{Fore.RESET}')
        return datafile

    else :
        print(f'PREPARING DATA DOWNLOAD. PLEASE WAIT ...', end='\r', flush=True)
        download_result = download_files_threads(df)
        downloaded_datafile = update_datafile_by_tmplog(datafile)
        update_result_into_tmpfile(datafile, 'download')
        update_result_into_tmpfile(datafile, 'status')
        update_result_into_tmpfile(datafile, 'size')

        if len(download_result) :
            print(f'DATA DOWNLOAD DONE.', end='\r', flush=True)
            return downloaded_datafile
        else :
            raise ValueError(
                errno.ENOENT, os.strerror(errno.ENOENT), download_result)


def main():
    download_data_files(r'_DATAFILE_PATH_')

if __name__ == '__main__':
    main()