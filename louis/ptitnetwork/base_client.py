import requests
import logging
import json


logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class BaseHTTPClient:
    _headers = {'Content-Type': 'application/json'}

    def __init__(self, base_url, client=None):
        self._client = client or requests.Session()
        self._base_url = base_url

    def hander_error(self, e):
        logger.exception(e)

    def post(self, url, data, headers=None, query_string=None):
        params = self._prepare_params(url, headers, query_string, data)
        try:
            resp = self._client.post(**params)
            return resp
        except Exception as e:
            self.hander_error(e)

    def delete(self, url, data, headers=None, query_string=None):
        params = self._prepare_params(url, headers, query_string, data)
        try:
            resp = self._client.delete(**params)
            return resp
        except Exception as e:
            self.hander_error(e)

    def patch(self, url, data, headers=None, query_string=None):
        params = self._prepare_params(url, headers, query_string, data)
        try:
            resp = self._client.patch(**params)
            return resp
        except Exception as e:
            self.hander_error(e)

    def put(self, url, data, headers = None, query_string=None):
        params = self._prepare_params(url, headers, query_string, data)
        try:
            resp = self._client.put(**params)
            return resp
        except Exception as e:
            self.hander_error(e)

    def get(self, url, headers=None, query_string=None):
        params = self._prepare_params(url, headers, query_string)
        try:
            resp = self._client.get(**params)
            return resp
        except Exception as e:
            self.hander_error(e)

    def _prepare_params(self, url, headers, query_string, data=None):
        req_params = {"url": self._base_url + url,
                      "headers": headers or {},
                      "params": query_string if query_string else {}}

        if data:
            headers = {**req_params['headers'], **self._headers}
            req_params.update(
                headers=headers,
                data=json.dumps(data))

        return req_params
