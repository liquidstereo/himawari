import os, sys
from argparse import ArgumentParser

from process import write_datafile, download_data_files, concatenate_tiled_images
from process import show_result, preivew_result_images


def himawari(start_time: str, end_time: str, level: int,
             preview: bool=False) -> None:

    print('-')
    proc_datafile = write_datafile(start_time, end_time, level)
    download_result = download_data_files(proc_datafile)
    concatenate_result = concatenate_tiled_images(download_result)
    result = show_result(concatenate_result,
                         save_error=False,
                         show_error=True)
    print('-')

    if preview :
        preivew_result_images(result)

def main(args):
    start_time = args.start_time
    end_time = args.end_time
    level = args.level
    preview = args.preview
    himawari(start_time, end_time, level, preview)

if __name__ == '__main__':
    parser = ArgumentParser(add_help=False)

    parser.add_argument(
        '-s',
        '--start_time',
        type=str,
        default=None,
        required=True,
        help='START TIME (%Y-%m-%d %H:%M:%S) eg) 2024-01-01 00:00')

    parser.add_argument(
        '-e',
        '--end_time',
        type=str,
        default=None,
        required=True,
        help='END TIME (%Y-%m-%d %H:%M:%S) eg) 2024-12-31 23:50')

    parser.add_argument(
        '-lv',
        '--level',
        type=int,
        default=1,
        required=True,
        help='LEVEL OF DIMENSIONS (LEVEL CAN BE 1, 2, 4, 8, 16, 20)')

    parser.add_argument(
        '-p',
        '--preview',
        default=False,
        action='store_true',
        help='PREVIEW RESULT')

    args = parser.parse_args()
    main(args)