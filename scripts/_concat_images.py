
import os, sys
from os import PathLike
import cv2
import pandas as pd
import numpy as np
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed, wait
import concurrent.futures.thread
from typing import List, Union
from alive_progress import alive_bar

from configs import write_logger


# TODO: REFACTORING📌

# NOTE: SET THE MAXIMUM WORKERS🔥 / YOU MAY ADJUST ACCORDING TO YOUR NEEDS.😉
PROCESS_POOL = ProcessPoolExecutor()._max_workers

log_func = os.path.basename(__file__)


def list_to_array(tile_img_list: List[Union[str, PathLike]],
                  len_concat_imgs: int) -> List[list[Union[str, PathLike]]]:
    split_tilelist = np.array_split(tile_img_list, len_concat_imgs)
    array_list = [list(url) for url in split_tilelist]

    return array_list


def img_list_matrix(tlist: List[Union[str, PathLike]],
                    level: int) -> List[list[Union[str, PathLike]]]:
    matrx = []
    while tlist != []:
        matrx.append((tlist[:level]))
        tlist = (tlist[level:])

    return matrx


def concat_img_vh(list_2d: list) -> np.ndarray:
    return cv2.hconcat([cv2.vconcat(list_h) for list_h in list_2d])


def exec_concat_imgs(tile_img_list: List[Union[str, PathLike]],
                     concat_img_output: Union[str, PathLike],
                     level: int) -> Union[str, PathLike]:

    concat_dir = os.path.dirname(concat_img_output)
    os.makedirs(concat_dir, exist_ok=True)

    tile_img_matx = img_list_matrix(tile_img_list, level)
    concat_data_array = []

    for i in range( 0, len(tile_img_matx), 1 ):
        img_data_array = [cv2.imread(img) for img in tile_img_matx[i]]
        concat_data_array.append(img_data_array)
    concat_img_data = concat_img_vh(concat_data_array)
    cv2.imwrite(concat_img_output,
                concat_img_data,
                [cv2.IMWRITE_PNG_COMPRESSION, 3])  # <-- SET COMPRESSION QUALITY
    return concat_img_output


def process_callback(future: Union[str, PathLike]):
    result = future.result()
    write_logger(attr='info',
                 func=log_func,
                 msg={'path': result,'result': os.path.exists(result)})


def concatenate_images_multiple_threads(concat_output_path: List[Union[str, PathLike]],
                                        tile_img_list: List[Union[str, PathLike]],
                                        level: int) -> List[Union[str, PathLike]]:

    results = []  # <-- RESULT IMAGE PATH
    with ProcessPoolExecutor(max_workers=PROCESS_POOL) as executor, alive_bar(len(concat_output_path), title='CONCATENATING IMAGES ...  ') as bar:
        futures = [executor.submit(exec_concat_imgs, tile_img_list[i], concat_output_path[i], level) for i in range(len(concat_output_path))]
        for future in as_completed(futures):
            future.add_done_callback(process_callback)
            bar(1)
        wait(futures)
        results = [f.result() for f in futures]

    results = sorted(results)
    return results

if __name__ == '__main__':
    concatenate_images_multiple_threads()