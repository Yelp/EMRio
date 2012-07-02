import unittest
from tests.test_mockups import EC2
from tests.test_mockups import INSTANCE_COUNT
from tests.test_mockups import INSTANCE_NAME
from tests.test_mockups import OPTIMIZED_FILENAME
from emrio.EMRio import read_optimal_instances 

class TestEMRio(unittest.TestCase):

	def test_optimal_read(self):
		"""Reads the test instance file in to make sure reading optimal instances
		is working."""
		FILE_POOL = EC2.init_empty_reserve_pool()
		
		#Fill the pool with what should be in the test pool file.
		for utilization_class in FILE_POOL:
			FILE_POOL[utilization_class][INSTANCE_NAME] = INSTANCE_COUNT
		
		optimal_instances = read_optimal_instances(OPTIMIZED_FILENAME)
		self.assertEqual(optimal_instances, FILE_POOL)

