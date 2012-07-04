import unittest
import datetime
from unittest import TestCase

import pytz
# Setup a mock EC2 since west coast can be changed in the future.
from emrio.job_handler import no_date_filter, range_date_filter
from emrio.ec2_cost import EC2Info
from test_prices import COST, RESERVE_PRIORITIES
from emrio.config import TIMEZONE

EC2 = EC2Info(COST, RESERVE_PRIORITIES)

BASE_TIME = datetime.datetime(2012, 5, 20)
INTERVAL = datetime.timedelta(0, 3600)
INSTANCE_NAME = 'm1.small'
BASE_INSTANCES = 20
JOB = 'job1'


def create_test_job(instance_name, count, j_id, start_time=BASE_TIME,
	end_time=(BASE_TIME + INTERVAL)):
	"""Creates a test job dictionary that is similar to the structure of
	a normal job but with a lot less irrelevant data
	"""
	job1 = {'instancegroups': create_test_instancegroup(instance_name, count),
	'jobflowid': j_id, 'startdatetime': start_time, 'enddatetime': end_time}
	return job1


def create_test_instancegroup(instance_name, count):
	return [{'instancetype':instance_name, 'instancerequestcount':str(count)}]


class TestJobHandlerFunctions(TestCase):
	def test_malformed_jobs(self):
		"""If there are any jobs without a start or end date, no_date_filter removes them.
		This function puts 2 malformed jobs and a normal job into job flows, and only the
		normal job should remain afterwards.
		"""
		no_start_date_job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB)
		del no_start_date_job['startdatetime']
		no_end_date_job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB)
		del no_end_date_job['enddatetime']
		normal_job = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB)
		job_flows = [no_start_date_job, no_end_date_job, normal_job]
		job_flows_after = [normal_job]
		job_flows = no_date_filter(job_flows)
		self.assertEqual(job_flows, job_flows_after)

	def test_min_date_filter(self):
		"""This function specifies a min date and one of the jobs is out of the date range
		while the other isn't, so only one job should remain in the job_flows after
		"""
		basetime = BASE_TIME.replace(tzinfo=pytz.utc)
		outside_date = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB, start_time=basetime)
		min_date = "2012/05/21"
		min_date_datetime = datetime.datetime(2012, 5, 21)
		min_date_datetime = min_date_datetime.replace(tzinfo=TIMEZONE)
		normal_date = create_test_job(INSTANCE_NAME, BASE_INSTANCES, JOB, start_time=min_date_datetime)
		job_flows_after = [normal_date]
		job_flows = [outside_date, normal_date]
		job_flows = range_date_filter(job_flows, min_date, None)		
		self.assertEqual(job_flows, job_flows_after)

if __name__ == '__main__':
	unittest.main()
