import copy


class BaseException(Exception):
    default_sub_code = None
    default_status_code = None
    default_message = ''

    def __init__(self, message='', status_code=None, sub_code=None):
        self.message = message or self.default_message
        self.sub_code = sub_code or self.default_sub_code
        self.status_code = status_code or self.default_status_code

        super(BaseException, self).__init__(self.message)

    def to_dict(self):
        rv = copy.deepcopy(self.__dict__)
        rv.pop('status_code')
        return rv


class ServerError(BaseException):
    default_status_code = 500
    default_sub_code = 0


class ClientError(BaseException):
    default_status_code = 400
    default_sub_code = 3000


class InvalidToken(ClientError):
    default_status_code = 401
    default_sub_code = 3001
    default_message = 'Invalid token'


class MissingToken(InvalidToken):
    default_sub_code = 3002
    default_message = 'Missing token'


class TokenExpired(InvalidToken):
    default_sub_code = 3003
    default_message = 'Token expired'


InvalidToken.MissingToken = MissingToken
InvalidToken.TokenExpired = TokenExpired


class ResourceNotFound(ClientError):
    default_status_code = 404
    default_sub_code = 3004

    def __init__(self, resource, sub_code=None):
        message = f'Resource {resource} not found'
        super(ResourceNotFound, self).__init__(message, sub_code)


class BadRequest(ClientError):
    default_status_code = 400
    default_sub_code = 3005
    default_message = 'Bad request'


class ValidationError(BadRequest):
    default_sub_code = 3006

    def __init__(self, message, sub_code=None):
        super(ValidationError, self).__init__(message, sub_code)


BadRequest.ValidationError = ValidationError


class Duplication(ClientError):
    default_status_code = 409
    default_sub_code = 3007

    def __init__(self, message, sub_code=None):
        super(Duplication, self).__init__(message, sub_code)


class PermissionDenied(ClientError):
    default_status_code = 403
    default_sub_code = 3008
    default_message = 'Permissiom denied'


