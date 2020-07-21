from apscheduler.schedulers.asyncio import AsyncIOScheduler
import time
import datetime
import logging
import random


class Scheduler:

    def __init__(self, controller, start_time, runtime):
        self.scheduler = AsyncIOScheduler()
        self.controller = controller
        self.start_time = start_time
        self.runtime = runtime

    def start_scheduler(self):
        self.scheduler.start()

    async def stop_scheduler(self):
        self.scheduler.remove_all_jobs()
        self.scheduler.shutdown(wait=True)
        await self.controller.stop_client()

    def schedule_stop(self):
        dt = self.start_time + datetime.timedelta(seconds=self.runtime)
        self.scheduler.add_job(self.stop_scheduler, 'date', next_run_time=dt)

    def schedule_resource_measuring(self):
        self.scheduler.add_job(self.controller.start_resource_measuring, "date", next_run_time=self.start_time, args=(self.runtime, ))

    def schedule_time_series(self, time_series):
        for item in time_series:
            dt = self.start_time + datetime.timedelta(seconds=item[0])
            self.scheduler.add_job(self.controller.change_network_configuration, "date", next_run_time=dt, args=(item[1], item[2], item[3], ))

    def schedule_start_publishing(self, topic, timed_variable, value, settings):
        if timed_variable == "rate":
            dt = self.start_time + datetime.timedelta(seconds=(value / 1000) + random.random())
            self.scheduler.add_job(self.publish_rate, 'date', next_run_time=dt,
                                   args=(topic, value, timed_variable, settings, ))
        elif timed_variable == "exponentialDistribution":
            exp = random.expovariate(1/value)
            dt = self.start_time + datetime.timedelta(seconds=(exp / 1000) + random.random())
            self.scheduler.add_job(self.publish_exponential, 'date', next_run_time=dt,
                                   args=(topic, value, timed_variable, settings, ))
        logging.info("Scheduled Publishing to topic: %s", topic)

    def schedule_initiation(self, topic, timed_variable, value, settings):
        if timed_variable == "rate":
            start_time = datetime.datetime.fromtimestamp(time.time() + (value / 1000))
            self.scheduler.add_job(self.publish_rate, 'date', next_run_time=start_time,
                                   args=(topic, value, timed_variable, settings, ))
        elif timed_variable == "exponentialDistribution":
            exp = random.expovariate(1/value)
            start_time = datetime.datetime.fromtimestamp((time.time() + (exp / 1000)))
            self.scheduler.add_job(self.publish_exponential, 'date', next_run_time=start_time,
                                   args=(topic, value, timed_variable, settings, ))
        logging.info("Scheduled Publishing to topic: %s", topic)

    def schedule_reaction(self, topic, timed_variable, value, settings):
        if timed_variable == "fixedDelay":
            start_time = datetime.datetime.fromtimestamp(time.time() + (value / 1000))
            self.scheduler.add_job(self.publish_response, 'date', next_run_time=start_time, args=(topic, settings, ))
        elif timed_variable == "exponentialDistribution":
            exp = random.expovariate(1 / value)
            start_time = datetime.datetime.fromtimestamp((time.time() + (exp / 1000)))
            self.scheduler.add_job(self.publish_response, 'date', next_run_time=start_time, args=(topic, settings,))

    async def publish_rate(self, topic, value, timed_variable, settings):
        self.schedule_initiation(topic, timed_variable, value, settings)
        await self.controller.publish(topic, settings)

    async def publish_exponential(self, topic, value, timed_variable, settings):
        self.schedule_initiation(topic, timed_variable, value, settings)
        await self.controller.publish(topic, settings)

    async def publish_response(self, topic, settings):
        await self.controller.publish(topic, settings)
