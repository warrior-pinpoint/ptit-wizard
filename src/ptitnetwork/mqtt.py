from paho.mqtt import client
import time
import os
import uuid
from collections import namedtuple

from common import constant

_MQTT_CREDENTIAL = namedtuple('MQTT_CREDENTIAL', ['host', 'port', 'username', 'password'])


def on_connect_1(client, userdata, flags, rc):
    print('On connecting!', client)


class Mqtt(client.Client):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def create(cls, on_connect, post_fix=None, **kwargs):

        post_fix = post_fix or str(uuid.uuid4())
        post_fix = '-' + post_fix if not post_fix.startswith('-') else post_fix

        if 'client_id' not in kwargs:
            kwargs['client_id'] = '{}.{}-pip'.format(os.getpid(), post_fix)

        time_alive = kwargs.get('time_alive', 60)
        client = cls.__init__(kwargs.pop('client_id'))
        client.on_connect = on_connect

        cred = cls._infer_credential()
        client.username_pw_set(cred.username, cred.password)
        client.connect(cred.host, cred.port, time_alive)
        return client

    @staticmethod
    def _infer_credential():

        env_mapping = {
            constant.ENV_CREDENTIAL_PORT: "port",
            constant.ENV_CREDENTIAL_HOST: "host",
            constant.ENV_CREDENTIAL_USER: "username",
            constant.ENV_CREDENTIAL_PW: "password",
        }

        credential = {key: os.getenv(env_key) for env_key, key in env_mapping}
        credential = _MQTT_CREDENTIAL(**credential)
        return credential


if __name__ == '__main__':
    client = Mqtt.create('tai1')
    print(client)

    while True:
        time.sleep(1)