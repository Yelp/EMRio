"""This module is used for creating instance pools and calculating the
cost of running EC2 machines.

WARNING: If you are using this for calculations, the rates are pulled from
Amazon's website here: http://aws.amazon.com/ec2/reserved-instances/
The prices are calibrated for US West (Northern California).
if you want to change that, you need to create your own file in
ec2 folder and import it here.

If you need a reference, take a look at test/test_price.py

This class also creates instance pools and logs for storing the hours
machines run. Their structure is like this:
pool = {
	UTILIZATION_LEVEL: {
		INSTANCE_NAME: INSTANCE_COUNT
	}
}
UTILIZATION_LEVEL is the name of the utilization that Amazon uses:
		http://aws.amazon.com/ec2/reserved-instances/#2
INSTANCE_NAMES is the name that amazon uses for their instance types.
INSTANCE_COUNT is how many instances are 'bought' for that simulation.

Logs: are pools that count the amount of hours that instances run for,
	so instead of instance_count, instance_hours is stored.

"""

import copy


class EC2Info(object):
	"""This class is used to store EC2 info like costs from the config
	files a person supplies. All the functions in it use that config to build
	pools or calculate the costs of instances.
	"""

	def __init__(self, cost, reserve_priorities):
		"""Initializes an EC2 instance and ALL_PRIORITIES is
		all the utilization_class types including DEMAND.

		Args:
			cost: dict of all the costs (Comes from price configs)

			reserve_priorities: this is all the reserve utilization_classizations in sorted order
			or priorities.
		"""

		self.COST = cost
		self.RESERVE_PRIORITIES = reserve_priorities

		# Copy reserve_priorities since we want to preserve priority order.
		all_priorities = copy.deepcopy(reserve_priorities)

		for utilization_class in cost.keys():
			if utilization_class not in all_priorities:
				all_priorities.append(utilization_class)
		self.ALL_PRIORITIES = all_priorities

	def calculate_cost(self, logged_hours, pool):
		"""Calculates the total cost of the pool, and the amount of
		hours ran (logged_hours).

		Args:
			logged_hours: The amount of hours ran for each instance type on a certain
				utilization class level.

			pool: The amount of reserved instances bought for the logged_hours.

		Returns:
			cost: Cost of the pool and hourly costs for each of the logged_hours.
		"""
		# Calculate the upfront cost of all the instances.
		cost = 0.0
		for utilization_class in pool:
			for instance_type in pool[utilization_class]:
				cost += (
					self.COST[utilization_class][instance_type]['upfront'] *
					pool[utilization_class][instance_type]
				)
		# Hourly cost calculation
		for utilization_class in logged_hours:
			for instance_type in logged_hours[utilization_class]:
				cost += (
					self.COST[utilization_class][instance_type]['hourly'] *
					logged_hours[utilization_class][instance_type]
				)
		return cost

	def init_empty_reserve_pool(self):
		"""Creates an empty reserve pool.

		This takes all the reserve keys and creates an empty dictionary for
		all the utilization_class types specified in the cost config file.

		Returns:
			empty_pool: A pool that looks like this:
				pool= {utilization_class: {} }
		"""
		empty_pool = {}
		for utilization_class in self.RESERVE_PRIORITIES:
			empty_pool[utilization_class] = {}
		return empty_pool

	def init_empty_all_instance_types(self):
		"""This will create a dict of all instance types.

		Every utilization_class type will be initialized here, while reserve pool only does
		reserves.

		Returns:
			Same as init_empty_reserve_pool except for all utilization_classization types.
		"""
		empty_logged_hours = {}
		for utilization_class in self.ALL_PRIORITIES:
			empty_logged_hours[utilization_class] = {}
		return empty_logged_hours

	def init_reserve_counts(self):
		"""initializes counts for reserve utilization classes.

		The main use of this is to count the total instances bought. For
		a certain utilization_classization. The assumption is that this is calculating
		for a single instance_type, so that info doesn't need recording.

		Returns:
			A zero count of each utilization_classization type, like so:
			reserve_counts = { utilization_classIZATION_NAME: 0}
		"""
		reserve_counts = {}
		for utilization_class in self.RESERVE_PRIORITIES:
			reserve_counts[utilization_class] = 0
		return reserve_counts

	@staticmethod
	def instance_types_in_pool(pool):
		"""Gets the set of all instance types in
		a pool or log

		Args:
			pool: Instances currently owned for each utilization_classization type.

		Returns:
			A set of all the instances used for all utilization_classization types.
		"""
		instance_types = set()
		for utilization_class in pool:
			for instance_type in pool[utilization_class]:
				instance_types.add(instance_type)
		return instance_types

	def is_reserve_type(self, instance_type):
		"""This just returns if a utilization_classization type is
		a reserve instance. If not, it is probably DEMAND type.
		"""
		return instance_type in self.RESERVE_PRIORITIES

	def color_scheme(self):
		"""This creates a color scheme starting at red (bad) to
		green, with a slight hint of blue. This is used for graphing
		and having each utilization_classization type be a different color.

		Returns:
			colors: A dict where the key is the utilization_class name and the color is
				the color generated for that utilization_classization name.
		"""
		colors = {}
		red = 255
		green = 0
		blue = 240
		increment = 255 / (len(self.ALL_PRIORITIES) - 1)
		iterator = copy.deepcopy(self.ALL_PRIORITIES)
		iterator.reverse()  # This puts the worst up first.
		for utilization_class in iterator:
			red_hex = hex(red)[2:]
			green_hex = hex(green)[2:]
			blue_hex = hex(blue)[2:]
			# Hex skips a zero if the number is less than 10, so
			# this should add it back in.
			if green < 10:
				green_hex = '0' + green_hex
			if red < 10:
				red_hex = '0' + red_hex
			hex_color = '#' + red_hex + green_hex + blue_hex

			colors[utilization_class] = hex_color
			red = int(red - increment)
			green = int(green + increment)
		return colors

	@staticmethod
	def zero_instance_types(job_flows, pool):
		"""Use this function to 0 the instance pool
		with all the keys used in the job flows.

		example: if the job_flows has m1.small, and m1.large
		and we had 2 utils of LIGHT_UTIL and HEAVY_UTIL, the
		resultant pool from the function will be:

		pool = {
			LIGHT_UTIL: {
				'm1.small': 0, 'm1.large': 0
			}
			HEAVY_UTIL: {
				'm1.small': 0, 'm1.large': 0
			}
		}
		Args:
			pool: A dict of utilization level dictionaries with nothing in them.

		Mutates:
			pool: for each utilization type, it fills in all the instance_types
				that any job uses.
		"""
		for job in job_flows:
			for instance in job.get('instancegroups'):
				instance_type = instance.get('instancetype')
				for utilization_class in pool.keys():
					pool[utilization_class][instance_type] = 0

