import logging
import logging.config

from utils.log.config.logger_config import log_config

class CustomLogger:

    def __init__(self) -> None:

        logging.config.dictConfig(log_config)

        self.__logger = logging.getLogger("real_estate_etl")
        self.__set_logger_config()

       
    def __set_logger_config(self):
        self.__logger.setLevel(logging.INFO)


    def info(self, message: str):
        self.__logger.info(message, stacklevel=2)


    def warning(self, message: str):
        self.__logger.warning(message, stacklevel=2)


    def error(self, message: str):
        self.__logger.error(message, stacklevel=2)


    def critical(self, message: str):
        self.__logger.critical(message, stacklevel=2)


    def debug(self, message: str):
        self.__logger.debug(message, stacklevel=2)


    def fatal(self, message: str):
        self.__logger.fatal(message, stacklevel=2)
