from unittest import TestCase
import unittest
import optimizer
import datetime

from ec2_cost import EC2Info
from ec2.test_prices import *

EC2 = EC2Info(COST, RESERVE_PRIORITIES)
BASETIME = datetime.datetime(2012, 5, 20, 5)
MEDIUM_INTERVAL = datetime.timedelta(0, 50000)
LIGHT_DAY_INTERVAL = datetime.timedelta(0, 30000)
HEAVY_INTERVAL = datetime.timedelta(0, 80000)
DEMAND_INTERVAL = datetime.timedelta(0, 2000)
CURRENT_TIME = BASETIME
INSTANCE_NAME = 'm1.small'
BASE_INSTANCES = 10
JOB_AMOUNT = 5
DAY_INCREMENT = datetime.timedelta(1, 0)
EMPTY_POOL = EC2.init_empty_reserve_pool()


def create_parallel_jobs(amount, start_time=BASETIME,
	end_time=(BASETIME + HEAVY_INTERVAL), start_count=0):
		jobs = []
		for i in range(amount):
			test_job = create_test_job(INSTANCE_NAME, BASE_INSTANCES,
				str(i + start_count), end_time=end_time)
			jobs.append(test_job)
		return jobs


def create_test_job(instance_name, count, j_id, start_time=BASETIME,
	end_time=(BASETIME + HEAVY_INTERVAL)):
	job1 = {'instancegroups': create_test_instancegroup(instance_name, count),
	'jobflowid': j_id, 'startdatetime': start_time, 'enddatetime': end_time}
	return job1


def create_test_instancegroup(instance_name, count):
	return [{'instancetype':instance_name, 'instancerequestcount':str(count)}]


class TestOptimizeFunctions(TestCase):
	def test_heavy_util(self):
		"""This should just create a set of JOB_AMOUNT and then all
		will be assigned to heavy_util since default interval for jobs
		is 80 percent of a day (which is the total time the job flows run).
		"""
		current_jobs = create_parallel_jobs(JOB_AMOUNT)

		# Jobs done in parallel over long amounts of time should be additive.
		# This means that reserve will be the sum of all jobs.
		reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		self.assertEquals(optimized[HEAVY_UTIL], reserve_log)

	def test_medium_util(self):
		"""Same as heavy_util but with 40 percent of the day."""
		end_time = BASETIME + MEDIUM_INTERVAL
		current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)

		reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)

	def test_light_util(self):
		"""Same as heavy_util but with 30 percent of the day."""
		end_time = BASETIME + LIGHT_DAY_INTERVAL
		current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)

		reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		self.assertEquals(optimized[LIGHT_UTIL], reserve_log)

	def test_demand(self):
		"""All the jobs in parallel will not utilize enough time, this is equivalent
		to how some companies start up large amount of jobs at the same time. They
		should all go to demand utilization since they are spikes."""
		end_time = BASETIME + DEMAND_INTERVAL
		current_jobs = create_parallel_jobs(JOB_AMOUNT, end_time=end_time)

		empty_type = {INSTANCE_NAME: 0}
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		for util in optimized:
			self.assertEquals(optimized[util], empty_type)

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
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
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
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)
		self.assertEquals(optimized[HEAVY_UTIL], reserve_log)

	def test_all_reserve_types(self):
		"""By stacking parallel light, medium and heavy intervals, the optimizer
		should make it so that all instances are chosen of the interval amount.
		"""
		end_time = BASETIME + MEDIUM_INTERVAL
		end_time_light = BASETIME + LIGHT_DAY_INTERVAL
		current_jobs = create_parallel_jobs(JOB_AMOUNT)
		current_jobs.extend(create_parallel_jobs(JOB_AMOUNT, end_time=end_time,
												start_count=JOB_AMOUNT))
		current_jobs.extend(create_parallel_jobs(JOB_AMOUNT, end_time=end_time_light,
												start_count=JOB_AMOUNT * 2))
		reserve_log = {INSTANCE_NAME: BASE_INSTANCES * JOB_AMOUNT}
		optimized = optimizer.optimize_instance_pool(current_jobs, DAY_INCREMENT)
		self.assertEquals(optimized[MEDIUM_UTIL], reserve_log)
		self.assertEquals(optimized[HEAVY_UTIL], reserve_log)
		self.assertEquals(optimized[LIGHT_UTIL], reserve_log)

if __name__ == '__main__':
	unittest.main()
