import flask
from flask import jsonify, Response, request

from .utils import CustomJSONEncoder
from .utils import LoggerChannel, ErrorHandler
from .validators import required_args, required_body


"""
NOTE:
- before_request(f):
    App: Registers a function to run before each request and overriding all blueprint's before request hanlder
    Blueprint: Registers a function to executed before each request that is handled by a function of that blueprint.
    
- errorhandler(error_or_code)(f): 
    Blueprint 
        - Register a function to handle errors by code or exception that becomes active for this blueprint only.
    App:
        - If reregister a function for app level one more time, All of Blueprint's error handler will be overried.
    
You shoud try to have a deep understand about how Flask and Blueprint work together.  
"""


class Flask(flask.Flask):

    def __init__(self, *args, **kwargs):
        """
           Create a Flask app that has already been set up json encoder
        """
        super().__init__(*args, **kwargs)
        self.json_encoder = CustomJSONEncoder

    def unset_all_before_req_handlers(self, blueprint_name=None):
        """
        Remove funcs run before each request

        Args:
            - blueprint_name: name of blueprint that want to unset before handlers.
                If provided, all blueprint before hanlders will be removed, otherwise it will remove all
                app before request hanlders
        """
        self.before_request_funcs[blueprint_name] = []

    def set_error_handler(self, error_or_code, handler):
        self.errorhandler(error_or_code)(handler)

    def make_response(self, rv):
        """Suport return No content if view func return none
        """
        if rv is None:
            rv = ('', 204)

        elif isinstance(rv, tuple):
            if not isinstance(rv[0], Response):
                rv = list(rv)
                rv[0] = jsonify(rv[0])
                rv = tuple(rv)

        elif not isinstance(rv, Response):
            rv = jsonify(rv)

        return super(Flask, self).make_response(rv)


class Blueprint(flask.Blueprint):

    def set_error_handler(self, error_or_code, handler):
        """ Register a function to handle a error or code that has been raised in view function
        Params:
        - error_or_code: error or exception want to handle
        - handler: function handle above error.
        """
        self.errorhandler(error_or_code)(handler)


def create_default_app(*args, **kwargs):
    error_handler = ErrorHandler(error_channels=LoggerChannel())
    flask_app = Flask(*args, **kwargs)
    flask_app.set_error_handler(Exception, error_handler.handle)
    return flask_app


if __name__ == '__main__':
    flask_app = create_default_app(__name__)
    blue = Blueprint(__name__, 'v1')
    test_client = flask_app.test_client()

    @blue.route('/hi')
    def test_func():
        print("xyx")

    flask_app.register_blueprint(blue, url_prefix='/v1')

    test_client.get('/v1/hi')




