import datetime
from test_prices import COST, RESERVE_PRIORITIES
from emrio.ec2_cost import EC2Info

EC2 = EC2Info(COST, RESERVE_PRIORITIES)
OPTIMIZED_FILENAME = "tests/test_optimal_instances.txt"
INSTANCE_NAME = 'm1.small'
INSTANCE_COUNT = 20
BASE_TIME = datetime.datetime(2012, 5, 20, 5)
INCREMENT = datetime.timedelta(0, 3600)
INTERVAL = datetime.timedelta(0, 3600)
JOB = 'job1'
JOB_AMOUNT = 5 # Amount of jobs to run in parallel.

# All different times associated with utilization classes for a single day.
# Example: MEDIUM_INTERVAL has an interval of 60% of a day.
LIGHT_INTERVAL = datetime.timedelta(0, 30000)
MEDIUM_INTERVAL = datetime.timedelta(0, 50000)
HEAVY_INTERVAL = datetime.timedelta(0, 80000)
DEMAND_INTERVAL = datetime.timedelta(0, 2000)


