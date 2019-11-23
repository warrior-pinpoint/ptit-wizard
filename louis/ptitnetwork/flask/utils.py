import logging

from flask import json, jsonify, request, g, app, current_app

from werkzeug.exceptions import HTTPException
from werkzeug.exceptions import InternalServerError
from ptiterror import exceptions


df_logger = logging.getLogger(__name__)


class CustomJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            if hasattr(obj, 'serialize'):
                return obj.serialize()

            if isinstance(obj, exceptions.BaseException):
                return obj.to_dict()

            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return json.JSONEncoder.default(self, obj)


class ErrorHandler:

    def __init__(self, error_channels=None):

        if error_channels and not isinstance(error_channels, list):
            error_channels = [error_channels]

        self._error_channels = error_channels or []

    def handle(self, error):
        """Convert all exception into a uniform exception type
        - If the raised errors belong to flask exception, it will convert to BaseException
        - If the raised errors is unkown like value error..., it will convert to ServerException
        Other will be kept original
        """
        if isinstance(error, (HTTPException, InternalServerError)):
            error = exceptions.BaseException(error.description, error.code)
        elif not isinstance(error, exceptions.BaseException):
            error = exceptions.ServerError(str(error))

        for channel in self._error_channels:
            channel.publish(error)

        return jsonify(error), error.status_code

    def add_channel(self, channel):
        """
        Add a channel to error handler
        """
        self._error_channels.append(channel)

    def add_channels(self, channels):
        """Add a list of channels to error handler
        """
        self._error_channels.extend(channels)


class LoggerChannel:
    def __init__(self, logger=None):
        self._logger = logger or df_logger

    def publish(self, error):
        self._logger.exception(error)
