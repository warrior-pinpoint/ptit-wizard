import logging
import os
import sys
import traceback
import json_log_formatter
from logging.handlers import RotatingFileHandler
from datetime import datetime
from functools import wraps, reduce


class CustomisedJSONFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message, extra, record):
        extra['message'] = "[{}] {}".format(record.name, message)
        extra['levelname'] = record.levelname

        if 'time' not in extra:
            extra['time'] = datetime.utcnow()

        if record.levelname == "ERROR":
            extra['message'] = "[{}] ".format(record.name) + \
                "[{}:{}] - ".format(record.filename, record.lineno) + \
                "{}".format(record.message)

        if record.exc_info:
            extra['error.stack'] = self.formatException(record.exc_info)

        return extra


def logger(name,
           maxBytes=1000000,
           backupCount=5):
    """ Note: this always log to files with level DEBUG
    """
    PRODUCTION = os.environ.get("PRODUCTION")

    file_formater = ('%(asctime)s %(levelname)s [%(name)s] '
                     '[%(filename)s:%(lineno)d] - %(message)s')
    file_handler = RotatingFileHandler("{}.log".format(name),
                                       maxBytes=maxBytes,
                                       backupCount=backupCount)
    file_handler.setFormatter(logging.Formatter(file_formater))

    print_formater = '%(asctime)s %(levelname)s - %(message)s'
    stream_formatter = CustomisedJSONFormatter() if PRODUCTION \
        else logging.Formatter(print_formater)
    stream_level = logging.INFO if PRODUCTION else logging.DEBUG
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(stream_formatter)
    stream_handler.setLevel(stream_level)

    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    return logger


def log_traceback(logger):
    tb = traceback.format_exc()
    logger.exception(tb)
    return tb


class EmptySpan():
    def __init__(self):
        pass

    def set_tag(self, tag_name, tag_value):
        pass

    def set_tags(self, tags):
        pass

    def set_traceback(self, limit=20):
        pass

    def finish(self):
        pass


if os.environ.get('DD_TRACER_ON'):
    trace = tracer.trace
else:
    _EMPTY_SPAN = EmptySpan()

    def trace(span, service):
        return _EMPTY_SPAN


def trace_wrap(service_name="", tags={}):
    """
    Decorator to perform a trace
    Automatic get the function __module__ if no `service_name` is given
    Args:
        service_name String The name of the service in `dot` separate style
        tags Dict Tags for this trace
    Example:
        @trace_wrap("some.module.service_name", {'client': "picky-0.1"})
        def trace_me():
            # Do something
            pass
    """
    def trace_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            service = service_name if service_name else func.__module__
            span = trace(func.__name__, service)
            if tags:
                span.set_tags(tags)
            try:
                func(*args, **kwargs)
            except:
                span.set_traceback()
                raise
            finally:
                span.finish()

        return wrapped_function

    return trace_decorator


if os.environ.get('DD_TRACER_ON'):
    current_span = tracer.current_span
else:
    def current_span():
        return _EMPTY_SPAN
