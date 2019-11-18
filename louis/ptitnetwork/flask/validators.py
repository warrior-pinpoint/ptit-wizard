from functools import wraps

from louis.ptiterror.exceptions import BadRequest
from flask import request


def required_body(*field_names):
    def http_request(handler):
        @wraps(handler)
        def check_(*args, **kwargs):
            body = request.json or {}
            for field in field_names:
                value = body.get(field)
                if not value:
                    raise BadRequest('Missing payload field: ' + field)
            return handler(*args, **kwargs)
        return check_
    return http_request


def required_args(*field_names):
    def http_request(handler):
        @wraps(handler)
        def check_(*args, **kwargs):
            params = request.args or {}
            for field in field_names:
                value = params.get(field)
                if not value:
                    raise BadRequest('Missing query prameter: ' + field)
            return handler(*args, **kwargs)
        return check_
    return http_request
