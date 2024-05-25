import os, errno
from os import PathLike
import pandas as pd
from pandas import DataFrame
from requests import Request, Session, Response, adapters, HTTPError, ConnectionError, Timeout, TooManyRedirects
from urllib.error import URLError
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from threading import Event
from alive_progress import alive_bar
from typing import List, Dict, Union

from configs import write_logger, retry
from ._proc_fwrite import write_tmplog
from ._common import chunks_gen


# NOTE: SET THE MAXIMUM WORKERS🔥 / YOU MAY ADJUST ACCORDING TO YOUR NEEDS.😉
THREAD_POOL = ThreadPoolExecutor()._max_workers
log_func = os.path.basename(__file__)

session = Session()
session.mount('https://',
              adapters.HTTPAdapter(pool_maxsize=THREAD_POOL,
                         pool_connections=THREAD_POOL,
                         max_retries=5,
                         pool_block=False))



def get_url_status(url: str) -> Response:

    hdr = {'Accept-Encoding': 'identity',
           'User-agent' : ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, '
                           'like Gecko)Chrome/120.0.0.0 Safari/537.36'),
           'Accept' : ('ext/html,application/xhtml+xml,'
                       'application/xml;q=0.9,image/avif,image/webp,image/apng,'
                       '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'),
           'Accept-Language' : 'en;q=0.9,ko,ja;q=0.8,zh-CN;q=0.7,zh;q=0.6'}

    response = session.get(url, headers=hdr, stream=True)
    if response.status_code != 200:
        write_logger(attr='error',
                     func=log_func,
                     msg={f'request failed, error code: {response.status_code}, url: {response.url}'})
    # if 500 <= response.status_code < 600:   # <- IF SERVER IS OVERLOADED ...
    #     time.sleep(120)

    return response



@retry((TimeoutError, URLError, HTTPError), tries=4, delay=2+random.random(), backoff=2)
def download_files(url: str,
                   img_path: Union[str, PathLike],
                   tmp_path: Union[str, PathLike]) -> dict:

    datasize = 0
    is_downloaded = False

    if os.path.isfile(img_path) :  # <-- IF FILE EXIST ALREADY ...
        result = {'url' : url,
                  'path' : img_path,
                  'status' : 200,
                  'size' : os.path.getsize(img_path),
                  'download' : True}

    else :
        response = get_url_status(url)
        status = response.status_code

        if status == 200 :
            img_path_dir = os.path.dirname(img_path)
            if not os.path.isdir(img_path_dir) :
                os.makedirs(os.path.dirname(img_path), exist_ok=True)

            datasize = int(response.headers.get('content-length'))

            with open(img_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            if os.path.isfile(img_path) :  # <-- AFTER A DOWNLOAD HAS BEEN COMPLETED ...
                download_size = os.path.getsize(img_path)
                is_downloaded = True if download_size == datasize else False

        result = {'url' : url,
                'path' : img_path,
                'status' : status,
                'size' : datasize,
                'download' : is_downloaded}

        write_tmplog(tmp_path, result)  # <-- WRITE TMPLOG FILE

    return result


def process_callback(future: Dict[str, str]):
    result = future.result()
    write_logger(attr = 'info',
                 func = log_func,
                 msg = result)


def download_files_job(urls: List[str],
                           tile_imgs: List[Union[str, PathLike]],
                           tmp_files: List[Union[str, PathLike]]) -> List[dict]:

    event = Event()
    chunk_by = 50  # <-- BREAK A LIST INTO CHUNKS OF SIZE 50
    futures = []
    results = []

    with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor, alive_bar(len(urls), title='DOWNLOAD DATA FILES ...   ') as bar:

        chunk_urls = list(chunks_gen(urls, chunk_by))
        chunk_tile_imgs = list(chunks_gen(tile_imgs, chunk_by))
        chunk_tmp_files = list(chunks_gen(tmp_files, chunk_by))

        for i in range( 0, len(chunk_urls), 1 ):
            futures = [executor.submit(download_files, chunk_urls[i][j], chunk_tile_imgs[i][j], chunk_tmp_files[i][j]) for j in range(len(chunk_urls[i]))]

            # *** SET DELAY *** #
            # event.clear()
            # event.wait(random.uniform(2,6))  # NOTE: SET DOWNLOAD DELAY (MIN, MAX). YOU MAY ADJUST ACCORDING TO YOUR NEEDS.
            # event.set()
            # *** SET DELAY *** #

            for future in as_completed(futures):
                future.add_done_callback(process_callback)
                bar(1)

            wait(futures)

            results.append([f.result() for f in futures])

    results = sum(results, [])
    results = sorted(results, key=lambda x: x['url'])

    return results


def download_results_confirm(download_results: List[dict]):

    for result in download_results :
        img_path = result['path']
        img_download = result['download']
        while not os.path.exists(img_path) and img_download:
            print('DATA PREPARATION IN PROGRESS, PLEASE WAIT...', end='\r', flush=True)
            time.sleep(3)  # <-- WAITING FOR A FILE TO FULLY DOWNLOAD.
        if os.path.isfile(img_path) and img_download:
            pass
        elif not img_download :
            pass
        else:
            raise ValueError(
                errno.ENOENT, os.strerror(errno.ENOENT), img_path)


def download_files_threads(df: DataFrame) -> List[dict]:

    urls = df['url'].to_list()
    tile_imgs = df['tile_img_path'].to_list()
    tmp_files = df['log_path'].to_list()

    results = download_files_job(urls, tile_imgs, tmp_files)
    download_results_confirm(results)

    return results


def main():
    download_files_threads(r'_FILE_PATH_')

if __name__ == '__main__':
    main()