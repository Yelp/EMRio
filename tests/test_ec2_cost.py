"""These are all the tests for ec2_cost to make sure it is properly calculating
cost
"""
import copy
import unittest
from collections import defaultdict

from emrio_lib.ec2_cost import EC2Info

HEAVY_UTIL = "Heavy Utilization"
MEDIUM_UTIL = "Medium Utilization"
LIGHT_UTIL = "Light Utilization"
DEMAND = "On Demand"

EC2 = EC2Info("tests/test_prices.yaml")

INSTANCE_NAME = 'm1.small'
INSTANCE_COUNT = 10
RUN_TIME = 100

MOCK_HEAVY_POOL = {
    HEAVY_UTIL: {
        INSTANCE_NAME: INSTANCE_COUNT
    }
}

MOCK_EMPTY_POOL = {
    HEAVY_UTIL: defaultdict(int),
    MEDIUM_UTIL: defaultdict(int),
    LIGHT_UTIL: defaultdict(int)
}

MOCK_EMPTY_LOG = copy.deepcopy(MOCK_EMPTY_POOL)
MOCK_EMPTY_LOG[DEMAND] = defaultdict(int)


class TestEC2Info(unittest.TestCase):

    def test_init_empty_pool(self):
        """Checks init_empty_reserve_pool returns a correct pool."""
        empty_pool = EC2.init_empty_reserve_pool()
        self.assertEqual(MOCK_EMPTY_POOL, empty_pool)

    def test_init_all_reserve_types(self):
        """Checks init_empty_all_instance_types returns a correct
        empty log."""
        empty_log = EC2.init_empty_all_instance_types()
        self.assertEqual(MOCK_EMPTY_LOG, empty_log)

    def test_cost_heavy(self):
        """Checks the heavy utility upfront cost to make sure ec2
        is calculating correct amounts"""
        INSTANCE_UPFRONT_PRICE = (
            INSTANCE_COUNT * EC2.COST[HEAVY_UTIL][INSTANCE_NAME]['upfront'])
        cost, upfront_cost = (
            EC2.calculate_cost(MOCK_EMPTY_LOG, MOCK_HEAVY_POOL))
        self.assertEqual(INSTANCE_UPFRONT_PRICE, cost)
        self.assertEqual(upfront_cost, INSTANCE_UPFRONT_PRICE)

    def test_hourly_cost(self):
        """Checks to make sure EC2 cost is calculating the correct cost
        for logged hours"""

        # ARTEM: Is this better to make a mock, or should I make a const above?
        medium_logged_hours = copy.deepcopy(MOCK_EMPTY_LOG)
        medium_logged_hours[MEDIUM_UTIL][INSTANCE_NAME] = RUN_TIME
        medium_pool = copy.deepcopy(MOCK_EMPTY_POOL)
        medium_pool[MEDIUM_UTIL][INSTANCE_NAME] = INSTANCE_COUNT

        cost = (EC2.COST[MEDIUM_UTIL][INSTANCE_NAME]['hourly']
            * medium_logged_hours[MEDIUM_UTIL][INSTANCE_NAME])
        cost += (EC2.COST[MEDIUM_UTIL][INSTANCE_NAME]['upfront']
            * medium_pool[MEDIUM_UTIL][INSTANCE_NAME])
        ec2_cost, _ = EC2.calculate_cost(medium_logged_hours, medium_pool)

        # Actual cost calculated by hand. for m1.small
        explicit_cost = 1603.1
        self.assertEqual(cost, ec2_cost)
        self.assertEqual(cost, explicit_cost)
