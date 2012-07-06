import copy
import unittest
import datetime
from unittest import TestCase

from emrio_lib.ec2_cost import EC2Info
from emrio_lib.simulate_jobs import Simulator
from test_prices import HEAVY_UTIL, MEDIUM_UTIL, LIGHT_UTIL
from test_prices import DEMAND

EC2 = EC2Info("tests/test.yaml")

STARTING_TIME = datetime.datetime(2012, 5, 20, 5)
INTERVAL = datetime.timedelta(0, 3600)  # Interval a job runs for.

INSTANCE_NAME = 'm1.small'
BASE_INSTANCES = 20
JOB_NAME = 'job1'

EMPTY_POOL = EC2.init_empty_reserve_pool()
EMPTY_LOG = EC2.init_empty_all_instance_types()
EMPTY_JOB_FLOWS = []

HEAVY_POOL = {
	HEAVY_UTIL: {INSTANCE_NAME: BASE_INSTANCES},
	MEDIUM_UTIL: {},
	LIGHT_UTIL: {}
}

# Current jobs running.
JOBS_RUNNING = {
	JOB_NAME: {
		HEAVY_UTIL: {
			INSTANCE_NAME: BASE_INSTANCES
		}
	}

}


def create_test_job(instance_name, count, j_id, start_time=STARTING_TIME,
	end_time=(STARTING_TIME + INTERVAL)):
	"""Creates a test job dictionary that is similar to the structure of
	a normal job but with a lot less irrelevant data
	"""
	job1 = {'instancegroups': create_test_instancegroup(instance_name, count),
	'jobflowid': j_id, 'startdatetime': start_time, 'enddatetime': end_time}
	return job1


def create_test_instancegroup(instance_name, count):
	return [{'instancetype':instance_name, 'instancerequestcount':str(count)}]


class TestOptimizeFunctions(TestCase):
	def test_add_job(self):
		"""Will add a single job and test to make sure that the job stash is
		correct after the function runs.
		"""
		job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB_NAME)
		jobs_running = {}
		pool = HEAVY_POOL
		pool_used = copy.deepcopy(EMPTY_POOL)
		pool_used_after = HEAVY_POOL

		Simulator(EMPTY_JOB_FLOWS, pool, EC2).allocate_job(jobs_running, pool_used,
			job)
		self.assertEqual(jobs_running, JOBS_RUNNING)
		self.assertEqual(pool_used_after, pool_used)

	def test_remove_job(self):
		"""Do the exact opposite as test_add_job."""

		job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB_NAME)
		jobs_running = copy.deepcopy(JOBS_RUNNING)
		jobs_running_after = {}
		pool_used = copy.deepcopy(HEAVY_POOL)
		pool_used_after = EMPTY_POOL

		Simulator(EMPTY_JOB_FLOWS, EMPTY_POOL, EC2).remove_job(jobs_running,
			pool_used, job)
		self.assertEqual(jobs_running, jobs_running_after)
		self.assertEqual(pool_used_after, pool_used)

	def test_rearrange_instances(self):
		""" This test will take a bunch of medium instances of a
		current job and put them into the available heavy util pool since
		there is space there and they are a higher priority.
		"""
		jobs_running = {
			JOB_NAME: {
				MEDIUM_UTIL: {
					INSTANCE_NAME: BASE_INSTANCES
				}
			}
		}
		pool = HEAVY_POOL
		pool_used = copy.deepcopy(EMPTY_POOL)
		job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB_NAME)
		Simulator(EMPTY_JOB_FLOWS, pool, EC2).rearrange_instances(jobs_running,
			pool_used, job)
		HEAVY_UTIL_USED = {
			INSTANCE_NAME: BASE_INSTANCES
		}
		self.assertEqual(pool_used[HEAVY_UTIL], HEAVY_UTIL_USED)
		self.assertEqual(JOBS_RUNNING, jobs_running)

	def test_log_hours(self):
		"""This just makes sure the log hours is working by adding
		the JOBS_RUNNING of heavy instances to the empty log hours.
		"""
		log = copy.deepcopy(EMPTY_POOL)
		simulator = Simulator(EMPTY_JOB_FLOWS, EMPTY_POOL, EC2)
		simulator.log_hours(log, JOBS_RUNNING, JOB_NAME)
		self.assertEqual(log, HEAVY_POOL)

	def test_log_hours_parallel(self):
		"""This test will run 3 jobs in the same util in parallel.
		the shared pool should only be able to handle one of the three
		jobs, so the other two job times will be put in demand.
		"""
		test_job1 = create_test_job(INSTANCE_NAME, BASE_INSTANCES, 'j1')
		test_job2 = create_test_job(INSTANCE_NAME, BASE_INSTANCES, 'j2')
		test_job3 = create_test_job(INSTANCE_NAME, BASE_INSTANCES, 'j3')
		current_jobs = [test_job1, test_job2, test_job3]

		# Jobs done in parallel should be instance pool in reserved and
		# anything leftover will be in demand.
		reserve_log = {INSTANCE_NAME: BASE_INSTANCES}
		demand_log = {INSTANCE_NAME: BASE_INSTANCES * 2}
		log = Simulator(current_jobs, HEAVY_POOL, EC2).run()
		self.assertEqual(log[HEAVY_UTIL], reserve_log)
		self.assertEqual(log[DEMAND], demand_log)

	def test_simulator_sequential(self):
		"""This job runs three jobs sequentially. This is
		to test whether or not having a constant instance pool
		will take all jobs. Since all the jobs can fit in the pool,
		there should only be reserved hourly useage.
		"""
		current_time = STARTING_TIME

		test_job1 = create_test_job(INSTANCE_NAME, BASE_INSTANCES,
			'j1', current_time, (current_time + INTERVAL))
		current_time += INTERVAL
		test_job2 = create_test_job(INSTANCE_NAME, BASE_INSTANCES,
			'j2', current_time, (current_time + INTERVAL))
		current_time += INTERVAL
		test_job3 = create_test_job(INSTANCE_NAME, BASE_INSTANCES,
			'j3', current_time, (current_time + INTERVAL))
		current_time += INTERVAL
		current_jobs = [test_job1, test_job2, test_job3]

		# Jobs done in parallel should be instance pool in reserved and
		# anything leftover will be in demand.
		reserve_log = {INSTANCE_NAME: BASE_INSTANCES * len(current_jobs)}
		demand_log = {}
		log = Simulator(current_jobs, HEAVY_POOL, EC2).run()
		self.assertEqual(log[HEAVY_UTIL], reserve_log)
		self.assertEqual(log[DEMAND], demand_log)

	def test_empty_jobs(self):
		"""If there are no jobs, the simulation produces an empty log."""
		current_jobs = []
		log = Simulator(current_jobs, HEAVY_POOL, EC2).run()
		self.assertEqual(log, EMPTY_LOG)

	def test_empty_pool(self):
		"""An empty pool (pool = {}) is malformed and should raise an error."""
		current_jobs = [create_test_job(INSTANCE_NAME, BASE_INSTANCES, 'j1')]
		try:
			Simulator(current_jobs, {}, EC2).run()
			self.assertTrue(False)  # This shouldn't execute.
		except KeyError:
			self.assertTrue(True)

if __name__ == '__main__':
	unittest.main()
