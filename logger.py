import logging


log_format = " %(message)s"


file_handler = logging.FileHandler('results.logs')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(log_format))

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter(log_format))


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger
