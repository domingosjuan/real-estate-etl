import logging

class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < 40