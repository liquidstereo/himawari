from configs.loggercfg import Logger
def write_logger(attr: str, func: str, msg: dict[str, str]):
    _logger = Logger(attr)
    set_log = _logger.asfile(proc=func)
    eval(f'set_log.'+attr+'('+str(msg)+')')