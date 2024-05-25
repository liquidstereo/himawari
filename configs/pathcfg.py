import os
from os import PathLike
from typing import Union

# TODO: AWAITING REFACTORING 📌

def _abs_path(pth: Union[str, PathLike], abs: bool=True) -> Union[str, PathLike]:
    return os.path.abspath(os.path.join('./', pth)).replace(os.sep, '/') if abs else pth

class WorkDirPaths:
    # =========================================================================== #
    # Set Dir Paths                                                               #
    # =========================================================================== #
    set_root = os.path.abspath(os.path.join(__file__, *(['..'] * 2))).replace(os.sep, '/')
    set_data_dir = 'process/data'         # data file
    set_tile_img_dir = 'process/tmp/img'  # tile images
    set_concat_img_dir = 'result'         # result (concatenated images)
    set_tmplog_dir = 'process/tmp/log'    # tmplog(csv) files
    set_error_dir = 'process/error'       # error files
    set_tmp_dir = 'process/tmp'           # tmp files
    set_log_dir = 'process/logs'          # log files

    def __init__(self):
        self.reset()

    def __str__(self):
        names = ['root_dir',
                 'data_dir',
                 'tile_img_dir',
                 'concat_img_dir',
                 'tmplog_dir',
                 'error_dir',
                 'tmp_dir',
                 'log_dir']
        dirs = [self.__root,
                self.__data_dir,
                self.__tile_img_dir,
                self.__concat_img_dir,
                self.__tmplog_dir,
                self.__error_dir,
                self.__tmp_dir,
                self.__log_dir]
        return str(dict(zip(names, dirs)))

    def reset(self):
        self.__root = self.set_root
        self.__data_dir = self.set_data_dir
        self.__tile_img_dir = self.set_tile_img_dir
        self.__concat_img_dir = self.set_concat_img_dir
        self.__tmplog_dir = self.set_tmplog_dir
        self.__error_dir = self.set_error_dir
        self.__tmp_dir = self.set_tmp_dir
        self.__log_dir = self.set_log_dir

    def _dir_path(self, query: str) -> Union[str, PathLike]:
        pth = {
            'root': self.__root,
            'data': os.path.join(self.__root, self.__data_dir),
            'tile_img': os.path.join(self.__root, self.__tile_img_dir),
            'concat_img': os.path.join(self.__root, self.__concat_img_dir),
            'tmplog': os.path.join(self.__root, self.__tmplog_dir),
            'error': os.path.join(self.__root, self.__error_dir),
            'tmp': os.path.join(self.__root, self.__tmp_dir),
            'log': os.path.join(self.__root, self.__log_dir)
        }.get(query, self.__root)
        return _abs_path(pth)

    @property
    def root_dir(self) -> Union[str, PathLike]:
        return self._dir_path('root')
    @property
    def data_dir(self) -> Union[str, PathLike]:
        return self._dir_path('data')
    @property
    def tile_dir(self) -> Union[str, PathLike]:
        return self._dir_path('tile_img')
    @property
    def concat_dir(self) -> Union[str, PathLike]:
        return self._dir_path('concat_img')
    @property
    def tmplog_dir(self) -> Union[str, PathLike]:
        return self._dir_path('tmplog')
    @property
    def error_dir(self) -> Union[str, PathLike]:
        return self._dir_path('error')
    @property
    def tmp_dir(self) -> Union[str, PathLike]:
        return self._dir_path('tmp')
    @property
    def log_dir(self) -> Union[str, PathLike]:
        return self._dir_path('log')