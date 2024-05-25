import os, sys
from os import PathLike
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from typing import Union

sys.path.append('../')
from configs import WorkDirPaths        # Set Paths Configs
from scripts import remove_exist, abs_path


# =========================================================================== #
# IMPORT DIR PATH                                                             #
# =========================================================================== #
root_ = WorkDirPaths()
tmp_dir = root_.tmp_dir
logger_dir = root_.log_dir


def get_error_dataframe(datafile: Union[str, PathLike],
                        df: pd.DataFrame,
                        save_error: bool=False) -> pd.DataFrame:
    err_idx = df[df['download'] == False].index.to_list()
    err_df = df.loc[err_idx]
    if save_error :  # <-- "save_error= True" ALLOWS YOU TO SAVE THE ERROR DATAFRAME SEPARATELY.
        errorfile = datafile.replace('data', 'error')
        os.makedirs(os.path.dirname(errorfile), exist_ok=True)
        pa_table = pa.Table.from_pandas(err_df, preserve_index=False)
        pq.write_table(pa_table, errorfile, compression='gzip')

    return err_df


def remove_tmpfiles(activate: bool=True) -> None:
    if activate :  # <-- REMOVE EXIST TMP FILES
        if os.path.isdir(tmp_dir) :
            remove_exist(tmp_dir)
    else :
        print(f'tmp: {tmp_dir}')


def show_result(datafile: Union[str, PathLike],
                save_error: bool=False,
                show_error: bool=False) -> Union[str, PathLike]:

    tmpfile = datafile.replace('data', 'tmp')

    if os.path.isfile(tmpfile):
        df = pd.read_parquet(path=tmpfile, engine='pyarrow')
    else :
        df = pd.read_parquet(path=datafile, engine='pyarrow')

    error_df = get_error_dataframe(datafile, df, save_error)
    filtered_df = df[~df.index.isin(error_df.index)]

    result_df_idx = filtered_df.index.to_list()

    query_count = len(df['url'])
    error_count = len(error_df['download'].to_list())
    download_count = len(result_df_idx)
    result_images = list(set(filtered_df['concat_img_path'].to_list()))
    result_count = len(result_images)

    result_dict = {
        'query' : query_count,
        'download' : download_count,
        'error' : error_count,
        'result' : result_count,
        'result.dir' : list(set([(os.path.dirname(p)) for p in df['concat_img_path']]))
    }

    result_as_df = pd.DataFrame(data=result_dict, index=[0])

    print(result_as_df.to_markdown(index=False, tablefmt='outline'))

    if error_count :
        error_log_dir = abs_path(os.path.join(logger_dir, 'error'))
        print(f'\n{error_count} ERROR FOUND. PLEASE CHECK LOG FILE FOR MORE DETAILS. '
              f'(\"{error_log_dir}\")')

    elif error_count and show_error :  # <-- SHOW ERRORS
        error_index = error_df['index'].to_list()
        error_download = error_df['download'].to_list()
        error_localtime = error_df['localtime'].to_list()
        error_utc = error_df['utc'].to_list()
        error_status = error_df['status'].to_list()

        error_dict = {'error index':error_index,
                      'error localtime': error_localtime,
                      'error utc': error_utc,
                      'download': error_download,
                      'error status': error_status}
        error_as_df = pd.DataFrame(data=error_dict)
        print('-\n', error_as_df.to_markdown(index=False, tablefmt='simple'))

    remove_tmpfiles()  # <-- REMOVE ALL TMP FILES

    return datafile



def main():
    show_result(r'_FILE_PATH_', show_error=True)

if __name__ == '__main__':
    main()