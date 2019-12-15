import schedule as baseSchedule
from schedule import ScheduleError, ScheduleValueError, CancelJob
import datetime
import random
import threading
import time
import logging


logger = logging.getLogger(__name__)


class UTCScheduler(baseSchedule.Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._thread = None
        self._thread_terminate = True

    def timeout(self, interval=1):
        """
        Schedule a new one-time job.
        :param interval: A quantity of a certain time unit
        :return: An unconfigured :class:`OneTimeJob <OneTimeJob>`
        """
        job = OneTimeJob(interval, self)
        return job

    @property
    def idle_seconds(self):
        return (self.next_run - datetime.datetime.utcnow()).total_seconds()

    def schedule(self, setting):
        if 'every' or 'timeout' in setting:
            rate = setting.get('every') or setting.get('timeout')
            func_rate = 'every' if 'every' in setting else 'timeout'
            if isinstance(rate, str):
                # 'every': 'day'
                job = getattr(getattr(self, func_rate)(), rate)
            elif len(rate) == 2:
                # 'every': (5, 'hours)
                interval, time_unit = rate
                job = getattr(getattr(self, func_rate)(interval), time_unit)
            else:
                # 'every': (10, 20, 'minutes')
                from_interval, to_interval, time_unit = rate
                job = getattr(getattr(self, func_rate)(from_interval).to(to_interval), time_unit)
        else:
            raise Exception('invalid input')

        if 'at' in setting:
            job.at(setting['at'])

        if 'ats' in setting:
            job = job.ats(*setting['ats'])

        return job

    def every(self, interval=1):
        job = UTCJob(interval, self)
        return job

    def loop_start(self, interval=1):
        if self._thread is not None:
            self.loop_stop()

        self._thread_terminate = False
        self._thread_interval = interval
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
            self.run_pending()

    def run_pending(self):
        runnable_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(runnable_jobs):
            try:
                self._run_job(job)
            except Exception as e:
                logger.error('exception while running scheduled jobs -- ' + str(e))
                debug.log_traceback(logger)


class JobList:
    def __init__(self):
        self._jobs = []

    def append(self, job):
        self._jobs.append(job)

    def do(self, *args, **kwargs):
        for job in self._jobs:
            job.do(*args, **kwargs)

        return self


class UTCJob(baseSchedule.Job):
    @property
    def should_run(self):
        return datetime.datetime.utcnow() >= self.next_run

    def ats(self, *time_str_list):
        if len(time_str_list) <= 0:
            raise ScheduleError('time_str_list size must be greater than 0')

        time_str_list = list(time_str_list)
        jobs = JobList()
        while len(time_str_list) > 1:
            job = UTCJob(self.interval, self.scheduler)
            job.unit = self.unit
            job.tags = self.tags
            job.at(time_str_list.pop())
            jobs.append(job)

        jobs.append(self.at(time_str_list[0]))
        return jobs

    def _schedule_next_run(self):
        """
        Compute the instant when this job should run next.
        """
        if self.unit not in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            raise ScheduleValueError('Invalid unit')

        if self.latest is not None:
            if not (self.latest >= self.interval):
                raise ScheduleError('`latest` is greater than `interval`')
            interval = random.randint(self.interval, self.latest)
        else:
            interval = self.interval

        self.period = datetime.timedelta(**{self.unit: interval})
        self.next_run = datetime.datetime.utcnow() + self.period
        if self.start_day is not None:
            if self.unit != 'weeks':
                raise ScheduleValueError('`unit` should be \'weeks\'')
            weekdays = (
                'monday',
                'tuesday',
                'wednesday',
                'thursday',
                'friday',
                'saturday',
                'sunday'
            )
            if self.start_day not in weekdays:
                raise ScheduleValueError('Invalid start day')
            weekday = weekdays.index(self.start_day)
            days_ahead = weekday - self.next_run.weekday()
            if days_ahead <= 0:  # Target day already happened this week
                days_ahead += 7
            self.next_run += datetime.timedelta(days_ahead) - self.period
        if self.at_time is not None:
            if (self.unit not in ('days', 'hours', 'minutes')
                    and self.start_day is None):
                raise ScheduleValueError(('Invalid unit without'
                                          ' specifying start day'))
            kwargs = {
                'second': self.at_time.second,
                'microsecond': 0
            }

            if self.unit == 'days' or self.start_day is not None:
                kwargs['hour'] = self.at_time.hour
            if self.unit in ['days', 'hours'] or self.start_day is not None:
                kwargs['minute'] = self.at_time.minute
            self.next_run = self.next_run.replace(**kwargs)
            # If we are running for the first time, make sure we run
            # at the specified time *today* (or *this hour*) as well
            if not self.last_run:
                now = datetime.datetime.utcnow()
                if (self.unit == 'days' and self.at_time > now.time() and
                        self.interval == 1):
                    self.next_run = self.next_run - datetime.timedelta(days=1)
                elif self.unit == 'hours' \
                        and self.at_time.minute > now.minute \
                        or (self.at_time.minute == now.minute
                            and self.at_time.second > now.second):
                    self.next_run = self.next_run - datetime.timedelta(hours=1)
                elif self.unit == 'minutes' \
                        and self.at_time.second > now.second:
                    self.next_run = self.next_run - \
                                    datetime.timedelta(minutes=1)
        if self.start_day is not None and self.at_time is not None:
            # Let's see if we will still make that time we specified today
            if (self.next_run - datetime.datetime.utcnow()).days >= 7:
                self.next_run -= self.period

    def cancel(self):
        self.scheduler.cancel_job(self)

    def run(self):
        ret = None
        try:
            ret = self.job_func()
        except Exception as e:
            logger.error('exception while execute job function -- ' + str(e))
            debug.log_traceback(logger)

        self.last_run = datetime.datetime.utcnow()
        self._schedule_next_run()
        return ret


class OneTimeJob(UTCJob):
    @property
    def today(self):
        return self.day

    def run(self):
        super().run()
        return CancelJob()

default_scheduler = UTCScheduler()

def timeout(interval=1):
    return default_scheduler.timeout(interval)

def every(interval=1):
    return default_scheduler.every(interval)

def tick():
    default_scheduler.run_pending()

def loop_start(interval=1):
    default_scheduler.loop_start(interval)

def loop_forever(interval=1):
    while True:
        default_scheduler.run_pending()
        time.sleep(interval)

def loop_stop():
    default_scheduler.loop_stop()

def clear(tag=None):
    default_scheduler.clear(tag)

def next_run():
    return default_scheduler.next_run

def idle_seconds():
    return default_scheduler.idle_seconds

def schedule(setting):
    return default_scheduler.schedule(setting)
