from pyspark.sql import SparkSession
from typing import Any

ROOT_LOGGER_NAME = "root"


class Log4J:
    def __init__(self, spark: SparkSession, logger_name: str | None = None):
        super().__init__()
        if logger_name:
            logger_name = ROOT_LOGGER_NAME + "." + logger_name
        else:
            logger_name = ROOT_LOGGER_NAME
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(logger_name)

    def debug(self, *msgs: Any):
        self.logger.debug(" ".join(map(str, msgs)))

    def info(self, *msgs: Any):
        self.logger.info(" ".join(map(str, msgs)))

    def warning(self, *msgs: Any):
        self.logger.warning(" ".join(map(str, msgs)))

    def error(self, *msgs: Any):
        self.logger.error(" ".join(map(str, msgs)))
