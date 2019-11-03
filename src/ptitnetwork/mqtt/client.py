from paho.mqtt import client
import time
import os
import uuid
from collections import namedtuple
from src.ptitnetwork.common import constants

MQTT_LOG_ERR = 0x08

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
        client = cls(client_id=kwargs.pop('client_id'))
        client.on_connect = on_connect

        cred = cls._infer_credential()
        client.username_pw_set(cred.username, cred.password)
        client.connect(cred.host, cred.port, time_alive)
        return client

    @staticmethod
    def _infer_credential():

        env_mapping = {
            constants.ENV_CREDENTIAL_PORT: "port",
            constants.ENV_CREDENTIAL_HOST: "host",
            constants.ENV_CREDENTIAL_USER: "username",
            constants.ENV_CREDENTIAL_PW: "password",
        }

        credential = {key: os.getenv(env_key) for env_key, key in env_mapping.items()}
        credential['port'] = int(credential['port'])
        credential = _MQTT_CREDENTIAL(**credential)
        return credential

    def message_callback_add(self, sub, callback):
        self.subscribe(sub)
        super().message_callback_add(sub, callback)

    def _handle_on_message(self, message):
        matched = False
        with self._callback_mutex:
            try:
                topic = message.topic
            except UnicodeDecodeError:
                topic = None

            if topic is not None:
                for callback in self._on_message_filtered.iter_match(
                        message.topic):
                    with self._in_callback_mutex:
                        callback(self, message)
                    matched = True

            if matched == False and self.on_message:
                with self._in_callback_mutex:
                    try:
                        self.on_message(self, self._userdata, message)
                    except Exception as err:
                        self._easy_log(MQTT_LOG_ERR,
                                       'Caught exception in on_message: %s',
                                       err)


def test():
    client1 = Mqtt.create(client_id='tai1', on_connect=on_connect_1)
    client2 = Mqtt.create(client_id='tai2', on_connect=on_connect_1)
    client1.loop_start()
    client2.loop_start()

    def tesy(client, message):
        print('xin chao cac bann')

    client1.message_callback_add('test', tesy)

    import json

    client2.publish('test', json.dumps({'helloL': 'ddd'}))

    while True:
        time.sleep(1)


if __name__ == '__main__':
    test()
