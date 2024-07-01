import sys

from utils.log.filter.logging_filter import _ExcludeErrorsFilter

log_config = {
    'version': 1,
    'filters': {
        'exclude_errors': {
            '()': _ExcludeErrorsFilter, 
        }
    },
    'formatters': {
        'custom_formatter': {
            'format': '%(name)s - %(levelname)s: %(asctime)s - %(message)s'
        }
    },
    'handlers': {
        'console_stderr': {
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'custom_formatter',
            'stream': sys.stderr
        },
        'console_stdout': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'custom_formatter',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        },
        'console_stdout': {
            'class': 'logging.StreamHandler',
            'level': 'WARN',
            'formatter': 'custom_formatter',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        },
        'console_stdout': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'custom_formatter',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console_stderr', 'console_stdout']
    }
}