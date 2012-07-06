from unittest import TestCase
import unittest
import datetime
import copy
from math import ceil

from emrio_lib.optimizer import Optimizer, convert_to_yearly_estimated_hours
from emrio_lib import ec2_cost
from test_prices import *

EC2 = ec2_cost.EC2Info("tests/test_prices.yaml")
print EC2.COST
BASETIME = datetime.datetime(2012, 5, 20, 5)

LIGHT_INTERVAL = datetime.timedelta(0, 30000)
MEDIUM_INTERVAL = datetime.timedelta(0, 50000)
HEAVY_INTERVAL = datetime.timedelta(0, 80000)
DEMAND_INTERVAL = datetime.timedelta(0, 2000)

INSTANCE_NAME = 'm1.small'
BASE_INSTANCES = 10
JOB_AMOUNT = 5
DAY_INCREMENT = datetime.timedelta(1, 0)

EMPTY_POOL = EC2.init_empty_reserve_pool()
EMPTY_LOG = EC2.init_empty_all_instance_types()

DEFAULT_LOG = copy.deepcopy(EMPTY_LOG)
DEFAULT_LOG[MEDIUM_UTIL][INSTANCE_NAME] = 100


def create_parallel_jobs(amount,
                        start_time=BASETIME,
                        end_time=(BASETIME + HEAVY_INTERVAL),
                        start_count=0):
        """Creates pseudo-jobs that will run in parallel with each other.

        Args:
            amount: Amount of jobs to run in parallel.

            start_time: a datetime all the jobs start at.

            end_time: a datetime all the jobs end at.

            start_count: job id's
        """
        jobs = []
        for i in range(amount):
            test_job = create_test_job(INSTANCE_NAME, BASE_INSTANCES,
                str(i + start_count), end_time=end_time)
            jobs.append(test_job)
        return jobs


def create_test_job(instance_name,
                    instance_count,
                    j_id,
                    start_time=BASETIME,
                    end_time=(BASETIME + HEAVY_INTERVAL)):
    """Creates a simple job dict object

    """
    job1 = {
        'instancegroups': create_test_instancegroup(instance_name,
                                                    instance_count),
        'jobflowid': j_id,
        'startdatetime': start_time,
        'enddatetime': end_time}

    return job1


def create_test_instancegroup(instance_name, count):
    return [{'instancetype':instance_name, 'instancerequestcount':str(count)}]


class TestOptimizeFunctions(TestCase):

    def test_upgraded_pool(self):
        """Makes sure that if a person already has instances
        purchased, that any upgraded pools will take that into
        account.

        In this example, the job pool should output 50 instances to
        HIGH UTILIZATION but since 50 instances of medium were already
        purchased, we can't upgrade to HIGH UTILIZATION. So the end
        result should be 0 HIGH UTILIZATION, and 50 MEDIUM_UTILIZATION.
        """
        current_jobs = create_parallel_jobs(JOB_AMOUNT)
        current_pool = EC2.init_empty_reserve_pool()
        current_pool[MEDIUM_UTIL][INSTANCE_NAME] = JOB_AMOUNT * BASE_INSTANCES

        heavy_util = EC2.init_empty_reserve_pool()[HEAVY_UTIL]
        heavy_util[INSTANCE_NAME] = 0
        medium_util = {INSTANCE_NAME: len(current_jobs) * BASE_INSTANCES}

        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run(
                pre_existing_pool=current_pool)

        self.assertEquals(optimized[HEAVY_UTIL], heavy_util)
        self.assertEquals(optimized[MEDIUM_UTIL], medium_util)

    def test_downgraded_pool(self):
        """This is the opposite of upgraded. If we have 50 heavy util
        and the suggested jobs should be 50 medium util with the new
        optimized pool, then don't suggest medium util, but keep heavy.
        """
        end_time = BASETIME + MEDIUM_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)
        current_pool = EC2.init_empty_reserve_pool()
        current_pool[HEAVY_UTIL][INSTANCE_NAME] = JOB_AMOUNT * BASE_INSTANCES

        medium_util = EC2.init_empty_reserve_pool()[HEAVY_UTIL]
        medium_util[INSTANCE_NAME] = 0
        heavy_util = {INSTANCE_NAME: len(current_jobs) * BASE_INSTANCES}

        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run(
                pre_existing_pool=current_pool)

        self.assertEquals(optimized[HEAVY_UTIL], heavy_util)
        self.assertEquals(optimized[MEDIUM_UTIL], medium_util)

    def test_heavy_util(self):
        """This should just create a set of JOB_AMOUNT and then all
        will be assigned to heavy_util since default interval for jobs
        is 80 percent of a day (which is the total time the job flows run).
        """
        current_jobs = create_parallel_jobs(JOB_AMOUNT)

        # Jobs done in parallel over long amounts of time should be additive.
        # This means that reserve will be the sum of all jobs.
        reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[HEAVY_UTIL], reserve_log)

    def test_medium_util(self):
        """Same as heavy_util but with 40 percent of the day."""
        end_time = BASETIME + MEDIUM_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)

        reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)

    def test_light_util(self):
        """Same as heavy_util but with 30 percent of the day."""
        end_time = BASETIME + LIGHT_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)

        reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[LIGHT_UTIL], reserve_log)

    def test_sequential_jobs(self):
        """The additive sum of sequential jobs should utilize 30 percent of
        a total day, which qualifies it for LIGHT_UTIL of the instance amount.
        The optimal value then is LIGHT_UTIL of instance amount.
        """
        current_jobs = []
        current_time = BASETIME
        interval = datetime.timedelta(0, 4000)
        end_time = current_time + interval
        for i in range(JOB_AMOUNT):
            job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, str(i),
                start_time=current_time, end_time=end_time)
            current_jobs.append(job)
            current_time = end_time
            end_time += interval
        reserve_log = {INSTANCE_NAME: BASE_INSTANCES}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[LIGHT_UTIL], reserve_log)
        pass

    def test_mixed_reserves(self):
        """Will mix medium interval and heavy interval to see if the optimizer
        chooses both types (which it should)"""
        end_time = BASETIME + MEDIUM_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT)
        current_jobs.extend(create_parallel_jobs(JOB_AMOUNT, end_time=end_time,
                                                start_count=JOB_AMOUNT))
        reserve_log = {INSTANCE_NAME: BASE_INSTANCES * JOB_AMOUNT}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)
        self.assertEquals(optimized[HEAVY_UTIL], reserve_log)

    def test_all_reserve_types(self):
        """By stacking parallel light, medium and heavy intervals, the
        optimizer should make it so that all instances are chosen of the
        interval amount.
        """
        end_time = BASETIME + MEDIUM_INTERVAL
        end_time_light = BASETIME + LIGHT_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT)
        current_jobs.extend(create_parallel_jobs(JOB_AMOUNT,
                                                end_time=end_time,
                                                start_count=JOB_AMOUNT))
        current_jobs.extend(create_parallel_jobs(JOB_AMOUNT,
                                                end_time=end_time_light,
                                                start_count=JOB_AMOUNT * 2))
        reserve_log = {INSTANCE_NAME: BASE_INSTANCES * JOB_AMOUNT}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)
        self.assertEquals(optimized[HEAVY_UTIL], reserve_log)
        self.assertEquals(optimized[LIGHT_UTIL], reserve_log)

    def test_zero_jobs(self):
        """This should return a zero instance pool since no jobs are ran"""
        no_jobs = []
        optimized = Optimizer(no_jobs, EC2, DAY_INCREMENT).run()
        self.assertEqual(optimized, EMPTY_POOL)

    def test_spikey_jobs(self):
        """This test will create a large number of job instances (100s) that
        run only a small period of time, making them poor choices for reserves.
        """
        end_time = BASETIME + DEMAND_INTERVAL
        current_jobs = create_parallel_jobs(JOB_AMOUNT * 100,
            end_time=end_time)

        empty_type = {INSTANCE_NAME: 0}
        optimized = Optimizer(current_jobs, EC2, DAY_INCREMENT).run()
        for util in optimized:
            self.assertEquals(optimized[util], empty_type)

    def test_interval_converter_two_months(self):
        """If using 2 months worth of data, it should multiply all the values
        by 6 to get a yearly prediction
        """
        logs = copy.deepcopy(DEFAULT_LOG)
        logs_after = copy.deepcopy(DEFAULT_LOG)

        # Converter is very precise, but by days, so it can't just be * 6,
        # more like 6.08333.
        logs_after[MEDIUM_UTIL][INSTANCE_NAME] = ceil(
            logs_after[MEDIUM_UTIL][INSTANCE_NAME] * (365.0 / 60.0))
        interval = datetime.timedelta(60, 0)
        convert_to_yearly_estimated_hours(logs, interval)
        self.assertEqual(logs, logs_after)

    def test_interval_converter_two_years(self):
        """Since we only want logs for one year, this should
        convert the hours to half the original hours."""
        logs = copy.deepcopy(DEFAULT_LOG)
        logs_after = copy.deepcopy(DEFAULT_LOG)
        logs_after[MEDIUM_UTIL][INSTANCE_NAME] = ceil(
            logs_after[MEDIUM_UTIL][INSTANCE_NAME] * (1.0 / 2.0))
        interval = datetime.timedelta(365.0 * 2, 0)
        convert_to_yearly_estimated_hours(logs, interval)
        self.assertEqual(logs, logs_after)

if __name__ == '__main__':
    unittest.main()
