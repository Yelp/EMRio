"""Tests for the main EMRio module are here."""
import unittest
from emrio_lib.ec2_cost import EC2Info
from emrio_lib.EMRio import read_optimal_instances
from test_prices import COST, RESERVE_PRIORITIES

EC2 = EC2Info(COST, RESERVE_PRIORITIES)
OPTIMIZED_FILE_NAME = "tests/test_optimal_instances.txt"
INSTANCE_NAME = 'm1.small'
INSTANCE_COUNT = 20
FILE_POOL = EC2.init_empty_reserve_pool()


class TestEMRio(unittest.TestCase):

	def test_optimal_read(self):
		"""Reads the test instance file in to make sure it is consistent."""

		for utilization_class in FILE_POOL:
			FILE_POOL[utilization_class][INSTANCE_NAME] = 20

		optimal_instances = read_optimal_instances(OPTIMIZED_FILE_NAME)
		self.assertEqual(optimal_instances, FILE_POOL)
