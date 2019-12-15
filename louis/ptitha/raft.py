"""
The Raft consensus algorithm.
Implementation follows the paper
Resources: https://raft.github.io/
"""
import logging
import time, random, os, json, threading
from louis.ptitnetwork.mqtt.client import Mqtt as MqttClient
from louis.ptithelper import scheduler, debug
from functools import wraps
from collections import deque

logger = debug.logger("[RAFT-MQTT]")


class State():
    """ Base State of Raft state machine
    """
    def __init__(self, machine, timeout, multiplier=2):
        """
        Args:
            - machine: the raft server
            - timeout: the lower timeout in second. used to determine the lower bound of timeout period
            - multiplier: the multiplier of timeout. used to determine the upper bound of timeout period
        """
        self.machine = machine
        self.timeout = timeout
        self.multiplier = multiplier if multiplier >= 1 else 1
        self.easy_log('init with timeout', self.timeout)

    @property
    def current_term(self):
        """shortcut to get current term from machine"""
        return self.machine._current_term

    @current_term.setter
    def current_term(self, term):
        """shortcut to set current term of machine """
        self.machine._current_term = term

    def _next_timeout(self):
        """ update the timeout time. the timeout period is defined by a random number better timeout lower and upperbound.
        lower bound timeout is defined when initalizing the state
        upper bound timeout is defined when initalize the state with the multiplier
        """
        self._cur_time = time.time()
        period = random.randint(self.timeout, self.timeout*self.multiplier) / 1000
        self._timeout_time =  self._cur_time + period
        return self._timeout_time

    def on_message(self, message):
        """called when there's a message
        this is where common behavior should be called regardless the message type
        """
        # self.easy_log('on message')
        term = message.payload['term']
        # If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if term > self.current_term:
            self.easy_log('receiving higher term')
            self.current_term = term
            self.machine.state = self.machine.follower
            return

    def on_append_entries(self, message):
        """called when there is an append entries request"""
        # self.easy_log('on append entries', message.payload)
        pass

    def on_append_replied(self, message):
        """called when the append entried request is replied (for leader)"""
        # self.easy_log('on append replied', message.payload)
        pass

    def on_request_vote(self, message):
        """called when there's a vote request"""
        # self.easy_log('on request vote', message.payload)
        pass

    def on_vote_received(self, message):
        """called when a vote request is replied (for candidate)"""
        # self.easy_log('on vote received', message.payload)
        pass

    def on_enter(self):
        """called when entering a new state upon state change"""
        self.easy_log('enter')
        pass

    def on_exit(self):
        """called when exiting an old state upon state change"""
        self.easy_log('exit')
        pass

    def on_timeout(self):
        """called when the timeout reaches"""
        self.easy_log('timeout')
        pass

    def easy_log(self, *messages):
        """easy log"""
        logger.info('[{}]'.format(self.__class__.__name__) + str(messages))
        pass

    def on_command(self, message):
        self.easy_log('on command')
        pass


class TaskWorker:
    MAX_INTERVAL = 2

    def __init__(self):
        self._tasks = deque()
        self._tasks_lock = threading.Lock()
        self._thread = None
        self._thread_terminate = True
        self._thread_interval = 0.5
        self._base_interval = 0.5

    def add_task(self, task):
        with self._tasks_lock:
            self._tasks.append(task)

    def loop_start(self, interval=0.5):
        if self._thread is not None:
            self.loop_stop()

        self._thread_terminate = False
        self._thread_interval = interval
        self._base_interval = interval
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def loop_stop(self):
        if self._thread is None:
            return

        self._thread_terminate = True
        if threading.current_thread() != self._thread:
            self._thread.join()
            self._thread = None

    def _loop(self):
        while not self._thread_terminate:
            time.sleep(self._thread_interval)
            task = None
            with self._tasks_lock:
                if len(self._tasks) > 0:
                    task = self._tasks.popleft()

            if task is not None:
                task()
                self._thread_interval = self._base_interval
            elif self._thread_interval < self.MAX_INTERVAL:
                self._thread_interval += 0.1


class Leader(State):
    def __init__(self, machine, timeout, worker, multiplier=1):
        # timeout should be less than regular timeout for hearbeat
        # multiplier should be 1 for the upper bound
        super(Leader, self).__init__(machine, timeout//4, multiplier)
        self._on_leader = None
        self._on_standby = None
        self._worker = worker

    def on_enter(self):
        super(Leader, self).on_enter()
        self._next_timeout()
        self.send_heart_beat()
        if self._on_leader:
            self._worker.add_task(self._on_leader)

    def on_exit(self):
        super().on_exit()
        if self._on_standby:
            self._worker.add_task(self._on_standby)

    def send_heart_beat(self):
        heart_beat = {
            'term': self.machine._current_term,
            'entries': [],
            'correlation_id': self.machine._id
        }

        self.machine._mqtt_client.publish(self.machine._entry_topic, json.dumps(heart_beat))

    def on_timeout(self):
        self._next_timeout()
        self.send_heart_beat()

    def on_command(self, message):
        super(Leader, self).on_command(message)
        # If command received from client: append entry to local log, respond after entry applied to state machine
        entry = {
            'term': self.current_term,
            'data': 'test'
        }

        self.machine._log.append(entry)
        self.machine.append_entry(entry)


class Candidate(State):
    def on_enter(self):
        super(Candidate, self).on_enter()
        # start new election upon state enter
        self.start_election()

    def start_election(self):
        # increse current term, vote for self, reset election timer, send
        self.current_term += 1
        self.easy_log('start new election with terms <{}>'.format(self.current_term))
        self._votes = [self.machine._id]

        if len(self._votes) > self.machine._num_member//2:
                self.machine.state = self.machine.leader

        self._next_timeout()
        self.machine._vote_for = self.machine._id
        self.machine.request_vote()

    def on_vote_received(self, message):
        super(Candidate, self).on_vote_received(message)

        # only check when vote is granted
        if not message.payload['success']:
            return

        # If votes received from majority of servers: become leader
        sender = message.payload['correlation_id']
        if sender not in self._votes:
            self._votes.append(sender)
            self.machine.response(message, True)
            if len(self._votes) > self.machine._num_member//2:
                self.machine.state = self.machine.leader

            return

    def on_timeout(self):
        super(Candidate, self).on_timeout()
        # If election timeout elapses: start new election
        self.start_election()

    def on_append_entries(self, message):
        # if append entries received form a leader, convert to follower
        super(Candidate, self).on_append_entries(message)
        self.machine.state = self.machine.follower


class Follower(State):
    def on_append_entries(self, message):
        super(Follower, self).on_append_entries(message)
        self._next_timeout()
        # Reply false if term < current_term
        term = message.payload['term']
        if term < self.machine._current_term:
            self.machine.response(message, False)
            return

    def on_timeout(self):
        super(Follower, self).on_timeout()
        # If election timeout elapses without receiving AppendEntries from current leader or granting vote to candidate: convert to candidate
        self.machine.state = self.machine.candidate

    def on_enter(self):
        super(Follower, self).on_enter()
        self._next_timeout()
        self.machine._vote_for = None

    def on_request_vote(self, message):
        super(Follower, self).on_request_vote(message)
        self._next_timeout()

        # response to candidate
        term = message.payload['term']
        # Reply false if term < currentTerm
        if term < self.machine._current_term:
            self.machine.response(message, False)
            return

        # If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
        sender = message.payload['correlation_id']
        vote_for = self.machine._vote_for
        if vote_for is None or vote_for == sender:
            self.machine._vote_for = sender
            self.machine.response(message, True)
            return


class Raft:

    DEFAULT_TIMEOUT_MS = 500
    DEFAULT_NUM_CLUSTER_MEMBERS = 3

    APPEND_ENTRIES_TOPIC = 'raft/{}/append-entries'
    REQUEST_VOTE_TOPIC = 'raft/{}/request-vote'
    COMMAND_TOPIC = 'raft/{}/command'
    REPLICATE_TOPIC = 'raft/{}/replicate'

    def __init__(self, cluster_id=None, member_id=None, timeout=DEFAULT_TIMEOUT_MS, num_members=DEFAULT_NUM_CLUSTER_MEMBERS):
        self._clus_id = cluster_id or os.getenv('CLUSTER_ID')
        member_id = member_id or os.getenv('HOST_NAME')
        self._id = '-'.join([member_id, str(os.getpid())])
        self._num_member = num_members
        self._is_leader = False
        logger.info('[RAFT] initialized with cluster {}'.format(self._clus_id))

        # two basic command
        self._entry_topic = Raft.APPEND_ENTRIES_TOPIC.format(self._clus_id)
        self._vote_topic = Raft.REQUEST_VOTE_TOPIC.format(self._clus_id)
        self._command_topic = Raft.COMMAND_TOPIC.format(self._clus_id)
        self._replicate_topic = Raft.REPLICATE_TOPIC.format(self._clus_id)

        # pubsub client
        self._mqtt_client = MqttClient.create(self.on_mqtt_connect, post_fix='raft')
        self._scheduler = scheduler.UTCScheduler()
        self._worker = TaskWorker()

        self._current_term = 0
        self._vote_for = None
        self._log = []

        # volatile state on all servers
        self._commit_index = 0
        self._last_applied = 0

        # three state of raft
        self.follower = Follower(self, timeout=timeout)
        self.candidate = Candidate(self, timeout=timeout)
        self.leader = Leader(self, timeout=timeout, worker=self._worker)
        self._state = None
        self._on_replicate_state = None

    def on_mqtt_connect(self, client, userdata, flags, rc):
        logger.info("[MQTT] connected with result code " + str(rc))
        # channels to broadcast request
        self._mqtt_client.message_callback_add(
            self._entry_topic, self.on_append_entries
        )
        self._mqtt_client.message_callback_add(
            self._vote_topic, self.on_request_vote
        )
        self._mqtt_client.message_callback_add(
            self._command_topic, self.on_command
        )
        self._mqtt_client.message_callback_add(
            self._replicate_topic, self.on_replicate
        )

        # channels to communicate directly with others
        self._mqtt_client.message_callback_add(
            self._entry_topic + '/reply-to/{}'.format(self._id), self.on_append_replied
        )
        self._mqtt_client.message_callback_add(
            self._vote_topic + '/reply-to/{}'.format(self._id), self.on_vote_received
        )

        self.state = self.follower
        self._scheduler.every(0.1).seconds.do(self.timeout_check)

    def on_append_entries(self, client, message):
        sender = message.payload['correlation_id']
        if sender == self._id:
            return
        self.state.on_message(message)
        self.state.on_append_entries(message)

    def on_request_vote(self, client, message):
        sender = message.payload['correlation_id']
        if sender == self._id:
            return
        self.state.on_message(message)
        self.state.on_request_vote(message)

    def on_append_replied(self, client, message):
        self.state.on_message(message)
        self.state.on_append_replied(message)

    def on_vote_received(self, client, message):
        self.state.on_message(message)
        self.state.on_vote_received(message)

    def on_command(self, client, message):
        self.state.on_command(message)

    def on_replicate(self, client, message):
        sender = message.payload.pop('correlation_id')
        if sender == self._id:
            return

        if self._on_replicate_state:
            self._on_replicate_state(message.payload)

    @property
    def on_replicate_state(self):
        return self._on_replicate_state

    @on_replicate_state.setter
    def on_replicate_state(self, func):
        self._on_replicate_state = func

    def loop_start(self):
        self._scheduler.loop_start(0.05)
        self._mqtt_client.loop_start()
        self._worker.loop_start()
        return self

    def loop_stop(self):
        self._scheduler.loop_stop()
        self._mqtt_client.disconnect()
        self._worker.loop_stop()
        return self

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        """state setter"""
        if state != self._state:
            if self._state:
                self._state.on_exit()

            self._state = state
            self._state.on_enter()
            self._is_leader = state == self.leader

    @property
    def on_leader(self):
        return self.leader._on_leader

    @on_leader.setter
    def on_leader(self, on_leader):
        self.leader._on_leader = on_leader
        return self

    @property
    def on_standby(self):
        return self.leader._on_standby

    @on_standby.setter
    def on_standby(self, on_standby):
        self.leader._on_standby = on_standby
        return self

    def timeout_check(self):
        if self.state._timeout_time - time.time() < 0:
            self.state.on_timeout()

    def on_message(self, client, message):
        self.state.on_message(message)

    def append_entry(self, entry=None):
        """NOTE: should be used by Leader only"""
        prev_index = 0
        prev_entry = {'term': self._current_term}
        if entry:
            prev_index = len(self._log) - 1
            prev_entry = self._log[prev_index] if prev_index > 0 else prev_entry
        message = {
            'term': self._current_term,
            'prev_log_idx': prev_index,
            'prev_log_term': prev_entry['term'],
            'entries': [] if not entry else [entry],
            'leader_commit_idx': self._commit_index,
            'correlation_id': self._id
        }

        self._mqtt_client.publish(self._entry_topic, json.dumps(message), qos=0)

    def request_vote(self):
        message = {
            'correlation_id': self._id,
            'term': self._current_term,
            'last_log_idx': 0,
            'last_log_term': 0,
        }

        self._mqtt_client.publish(self._vote_topic, json.dumps(message), qos=0)

    def response(self, message, success):
        reply_message = {
            'correlation_id': self._id,
            'term': self._current_term,
            'success': success
        }

        topic = message.topic + '/reply-to/' + message.payload['correlation_id']
        self._mqtt_client.publish(topic, json.dumps(reply_message), qos=0)

    def leader_only(self, function):
        """decorator to pause execution on node not being a leader"""
        @wraps(function)
        def callback(*args, **kwargs):
            if self._is_leader: return function(*args, **kwargs)

        return callback

    def replicate_state(self, state):
        if not self._is_leader:
            return

        assert isinstance(state, dict), "state has to be a dictionary"
        if 'correlation_id' not in state:
            state['correlation_id'] = self._id

        self._mqtt_client.publish(self._replicate_topic, json.dumps(state))


if __name__ == '__main__':
    Raft(num_members=5).loop_start()
    while True:
        time.sleep(1)
