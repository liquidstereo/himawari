import os
import logging
import datetime

from configs.pathcfg import WorkDirPaths

# =========================================================================== #
# IMPORT LOG DIR PATH                                                         #
# =========================================================================== #
root_ = WorkDirPaths()
logger_dir = root_.log_dir


class Logger:

    def __init__(self, attr=None):
        self.classname = 'Logger'
        if attr is None:
            self.attr = logger_dir
        else :
            self.attr = os.path.abspath(os.path.join(logger_dir, attr))


    def asfile(self, f=None, proc=None):
        os.makedirs(self.attr, exist_ok=True)
        now = datetime.datetime.now()
        if not f :
            f = 'proc.' + now.strftime("%Y-%m-%d") + '.log'  # <-- SET LOG NAME
        logfile = os.path.join(self.attr, f)

        self.logger = logging.getLogger(proc)
        if not logging._handlers:
            filehandler = logging.FileHandler(
                os.path.abspath(logfile),
                encoding='utf-8-sig',
                mode='a+')

        logformat = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | <%(name)s> : \n%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.logger.propagate = False
        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(filehandler)

        self.logger.setLevel(logging.DEBUG)
        filehandler.setFormatter(logformat)

        return self.logger