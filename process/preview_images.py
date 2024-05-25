import os, sys
from os import PathLike
import pandas as pd
import cv2
import numpy as np
from typing import List, Union, Dict

sys.path.append('../')
from scripts import datasort


def get_result_data(filepath: Union[str, PathLike]) -> Dict[str, str]:

    df = pd.read_parquet(path=filepath, engine='pyarrow')
    idx = df[df['download'] == True].index.to_list()

    localtime = list(set([(df.at[i,'localtime']) for i in idx]))
    utc = list(set([(df.at[i,'utc']) for i in idx]))
    concat_imgs = list(set([(df.at[i,'concat_img_path']) for i in idx]))
    concat_imgs = [f for f in concat_imgs if os.path.isfile(f)]

    localtime = datasort(localtime)
    utc = datasort(utc)
    concat_imgs = datasort(concat_imgs)

    result_dict = {'localtime': localtime, 'concat_imgs': concat_imgs}

    return result_dict


def insert_text(image: np.ndarray, text: str,
                font=cv2.FONT_HERSHEY_SIMPLEX,
                font_scale=1, font_thickness=1,
                text_color=(255, 255, 255), text_color_bg=(0, 0, 0),
                background=False) -> int:

    pos = (5, 515)
    x, y = pos
    text_size, _ = cv2.getTextSize(text, font, font_scale, font_thickness)
    text_w, text_h = text_size

    if background :
        bg_x = x+text_w
        bg_y = y+text_h
        bg_pos = pos
        cv2.rectangle(image, bg_pos,
                      (bg_x, bg_y),
                      text_color_bg, -1)

    cv2.putText(image, text,
                (x, y + text_h + font_scale),
                font, font_scale,
                text_color, font_thickness,
                cv2.LINE_AA)

    return text_size


def resize_image(image: np.ndarray, width: int=None, height: int=None,
                 inter=cv2.INTER_AREA) -> cv2.typing.MatLike:
    dim = None
    (h, w) = image.shape[:2]
    if width is None and height is None:
        return image
    if width is None:
        r = height / float(h)
        dim = (int(w * r), height)
    else:
        r = width / float(w)
        dim = (width, int(h * r))
    return cv2.resize(image, dim, interpolation=inter)


def exec_preivew_result(localtime_list: List[str],
                        image_list: List[Union[str, PathLike]],
                        ) -> None:

    print(f'PRESS THE ESC KEY TO END THE PREIVEW.', end='\r', flush=True)

    while True:
        is_quit=False
        while (True):
            for i in range( 0, len(image_list), 1 ):

                img_data = cv2.imread(image_list[i], cv2.IMREAD_COLOR)
                (h, w) = img_data.shape[:2]

                resize_result = resize_image(img_data, width=550)
                insert_text(resize_result, localtime_list[i])

                window_title = f'HIMAWARI RESULT ({w}px, {h}px)'
                cv2.imshow(window_title, resize_result)

                keys = cv2.waitKey(60) & 0xFF
                if keys == 27 :
                    is_quit=True
                    cv2.destroyAllWindows()
                    break

            win_prop = cv2.getWindowProperty(window_title, cv2.WND_PROP_VISIBLE)
            if win_prop <= 0:
                break

            else:
                break
        if is_quit:
            break
    cv2.destroyAllWindows()


def preivew_result_images(datafile: Union[str, PathLike]) -> None:
    result_data_dict = get_result_data(datafile)
    result_localtime = result_data_dict['localtime']
    result_images = result_data_dict['concat_imgs']
    exec_preivew_result(result_localtime, result_images)


def main():
    preivew_result_images('_FILE_PATH_')

if __name__ == '__main__':
    main()