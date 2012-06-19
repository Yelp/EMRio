"""This is the default EC2 cost calculator. EC2 has reserved instances and
on-demand instances. The reserved instances have an upfront cost but less
hourly rates. Each type with different utilization is expressed in the COST
dict supplied in ec2.west_coast_prices.
WARNING: If you are using this for calculations, the rates are pulled from
Amazon's website here: http://aws.amazon.com/ec2/reserved-instances/
The prices are calibrated for US West (Northern California).
if you want to change that, you need to create your own file in
ec2 folder and import it here.

The format for an ec2 config is as follows:
RESERVE_PRIORITIES - a list of all the reserve utilization types.
	the list needs to be in order, so the highest priority reserve type
	will be first in the list.

COST = {
	UTILIZATION_NAME: {
		INSTANCE_NAME: {
			'hourly': HOURLY_COST,
			'upfront': UPFRONT_COST,
		}
	}
}
UTILIZATION_NAME is for reserved and on-demand. You need to classify
	on demand as a type as well and just 0 out the upfront costs.
INSTANCE_NAME is the type of instance defined on Amazon EC2 pricing.
	(e.g. m1.small)
HOURLY_COST is the cost per hour of running the instance.
UPFRONT_COST is the cost to buy this machine for a year (0 for demand)

If you need a reference, take a look at ec2/test_price.py
"""

import copy
from ec2.west_coast_prices import COST, RESERVE_PRIORITIES


class EC2Info:
	"""This class is used to store EC2 info like costs from the config
	files a person supplies. All the functions in it use that config to build
	pools or calculate the costs of instances.
	"""

	def __init__(cls, cost, reserve_priorities):
		"""Initializes an EC2 instance and ALL_PRIORITIES is
		all the util types including DEMAND.

		Args:
			cost: dict of all the costs (look at comments in the beginning)

			reserve_priorities: this is all the reserve utilizations in sorted order
			or priorities.
		"""

		cls.COST = cost
		cls.RESERVE_PRIORITIES = reserve_priorities
		all_priorities = copy.deepcopy(reserve_priorities)

		for util in cost:
			if util not in all_priorities:
				all_priorities.append(util)
		cls.ALL_PRIORITIES = all_priorities

	def calculate_cost(cls, logs, pool):
		"""Calculates the total cost of the pool, and the amount of
		hours ran (logs).

		Args:
			logs: The amount of hours ran for each instance type on a certain
				utilization level.

			pool: The amount of reserved instances bought for the logs.

		Returns:
			cost: Cost of the pool and hourly costs for each of the logs.
		"""
		# Calculate the upfront cost of all the instances.
		cost = 0.0
		for util in pool:
			for i_type in pool[util]:
				cost += cls.COST[util][i_type]['upfront'] * pool[util][i_type]

		# Hourly cost calculation
		for util in logs:
			for i_type in logs[util]:
				cost += cls.COST[util][i_type]['hourly'] * logs[util][i_type]
		return cost

	def init_empty_reserve_pool(cls):
		"""Creates an empty reserve pool.

		This takes all the reserve keys and creates an empty dictionary for
		all the utilization types specified in the cost config file.

		Returns:
			empty_pool: A pool that looks like this:
				pool= {UTILIZATION_NAME: {} }
		"""
		empty_pool = {}
		for util in cls.RESERVE_PRIORITIES:
			empty_pool[util] = {}
		return empty_pool

	def init_empty_all_instance_types(cls):
		"""This will create a dict of all instance types.

		Every util type will be initialized here, while reserve pool only does
		reserves.

		Returns:
			Same as init_empty_reserve_pool except for all utilization types.
		"""
		empty_logs = {}
		for type in cls.ALL_PRIORITIES:
			empty_logs[type] = {}
		return empty_logs

	def init_reserve_counts(cls):
		"""Has counts of reserves instead of a deeper instance_type dict.

		The main use of this is to count the total instances bought. For
		a certain utilization. The assumption is that this is calculating
		for a single instance_type, so that info doesn't need recording.

		Returns:
			A zero count of each utilization type, like so:
			reserve_counts = { UTILIZATION_NAME: 0}
		"""
		reserve_counts = {}
		for util in cls.RESERVE_PRIORITIES:
			reserve_counts[util] = 0
		return reserve_counts

	def instance_types_in_pool(cls, pool):
		"""Gets the set of all instance types in
		a pool or log

		Args:
			pool: Instances currently owned for each utilization type.

		Returns:
			A set of all the instances used for all utilization types.
		"""
		instance_types = set()
		for util in pool:
			for i_type in pool[util]:
				instance_types.add(i_type)
		return instance_types

	def is_reserve_type(cls, instance_type):
		"""This just returns if a utilization type is
		a reserve instance. If not, it is probably DEMAND type.
		"""
		return instance_type in cls.RESERVE_PRIORITIES

	def color_scheme(cls):
		"""This creates a color scheme starting at red (bad) to
		green, with a slight hint of blue. This is used for graphing
		and having each utilization type be a different color.

		Returns:
			colors: A dict where the key is the util name and the color is
				the color generated for that utilization name.
		"""
		colors = {}
		red = 255
		green = 0
		blue_hex = 'F0'
		increment = 255 / (len(cls.ALL_PRIORITIES) - 1)
		iterator = copy.deepcopy(cls.ALL_PRIORITIES)
		iterator.reverse()  # This puts the worst up first.
		for util in iterator:
			red_hex = hex(red)[2:]
			green_hex = hex(green)[2:]

			# Hex skips a zero if the number is less than 10, so
			# this should add it back in.
			if green < 10:
				green_hex = '0' + green_hex
			if red < 10:
				red_hex = '0' + red_hex
			hex_color = '#' + red_hex + green_hex + blue_hex

			colors[util] = hex_color
			red = int(red - increment)
			green = int(green + increment)
		return colors


EC2 = EC2Info(COST, RESERVE_PRIORITIES)
