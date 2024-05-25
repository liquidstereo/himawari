import os, sys
from os import PathLike
import pyarrow as pa
import pyarrow.parquet as pq
import time
from typing import Union

sys.path.append('../')
from configs import WorkDirPaths
from configs import write_logger
from scripts import query_datafile_path, create_dataframe, append_exist_dataframe

# =========================================================================== #
# IMPORT DIR PATH FROM ../configs/pathcfg                                     #
# =========================================================================== #
root_ = WorkDirPaths()
data_dir = root_.data_dir
tile_img_dir = root_.tile_dir
concat_img_dir = root_.concat_dir
tmplog_dir = root_.tmplog_dir
# =========================================================================== #

def write_datafile(start_time: str, end_time: str, level: int) -> Union[str, PathLike]:

    df = create_dataframe(tile_img_dir, concat_img_dir, tmplog_dir,
                          start_time, end_time,
                          level)

    datafile = query_datafile_path(data_dir, start_time, level)
    if os.path.isfile(datafile) :
        df = append_exist_dataframe(datafile, df)
    pa_table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(pa_table, datafile, compression='gzip')

    time.sleep(1)

    if os.path.isfile(datafile) :
        write_logger(attr='info',
                     func=os.path.basename(__file__),
                     msg={f'result':os.path.exists(datafile), 'file': datafile})

    return datafile


def main():
    write_datafile('YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM', 'LEVEL')  # <-- START_TIME, END_TIME, LEVEL
if __name__ == '__main__':
    main()