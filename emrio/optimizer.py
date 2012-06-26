"""The optimizer module holds all the functions relating to creating the
best instance pool that yields the least cost over an interval of time.
"""
from math import ceil
from simulate_jobs import Simulator

from config import EC2

class Optimizer(object):
	def __init__(self, job_flows, job_flows_interval=None):
		self.job_flows = job_flows
		self.job_flows_interval = job_flows_interval
		if job_flows_interval is None:
			min_time = min(job.get('startdatetime') for job in job_flows)
			max_time = max(job.get('enddatetime') for job in job_flows)
			self.job_flows_interval = max_time - min_time

	def run(self):
		"""Take all the max_instance counts, then use that to combinatorically
		find the most cost efficient instance cost

		Returns: 
			optimal_pool: dict of the best pool of instances to be used.
		"""

		optimized_pool = EC2.init_empty_reserve_pool()

		# Zero-ing the instances just makes it so the optimized pool
		# knows all the instance_types the job flows use beforehand.
		EC2.zero_instance_types(self.job_flows, optimized_pool)
		for instance in EC2.instance_types_in_pool(optimized_pool):
			self.brute_force_optimize(instance, optimized_pool)
		return optimized_pool

	def brute_force_optimize(self, instance_type, pool):
		"""The brute force approach will take a single instance type and optimize the
		instance pool for it. By using the job_flows in simulations.

		Mutates: pool
		"""
		simulator = Simulator(self.job_flows, pool)
		previous_cost = float('inf')
		current_min_cost = float("inf")
		current_cost = float('inf')
		current_min_instances = EC2.init_reserve_counts()

		# Calculate the default cost first.
		logged_hours = simulator.run()
		convert_to_yearly_estimated_hours(logged_hours, self.job_flows_interval)
		current_min_cost = EC2.calculate_cost(logged_hours, pool)
		current_cost = current_min_cost

		while previous_cost >= current_cost:
			current_simulation_costs = EC2.init_reserve_counts()
			for utilization_class in current_simulation_costs:
				current_simulation_costs[utilization_class] = float('inf')

			# Add a single instance to each utilization type, and record the costs.
			# whichever is the minimum, and choose it.
			for utilization_class in pool:
				# Reset the min instances to the best values.
				for current_util in pool:
					pool[current_util][instance_type] = current_min_instances[current_util]
				pool[utilization_class][instance_type] = (
						current_min_instances[utilization_class] + 1)
				logged_hours = simulator.run()
				convert_to_yearly_estimated_hours(logged_hours, self.job_flows_interval)
				current_simulation_costs[utilization_class] = EC2.calculate_cost(
					logged_hours,
					pool)

			previous_cost = current_cost
			current_cost = min(current_simulation_costs.values())
			min_util_level = None
			for utilization_class in current_simulation_costs:
				if current_simulation_costs[utilization_class] == current_cost:
					min_util_level = utilization_class

			# Record the new cost, then check to see if adding one instance is better
			# If it is not, then break from the loop, since adding more will be worst.
			if min(current_cost, current_min_cost) != current_min_cost or (
			current_cost == current_min_cost):

				current_min_cost = current_cost
				current_min_instances[min_util_level] += 1
			# Reset to best instance pool.
			for current_util in pool:
				pool[current_util][instance_type] = (
					current_min_instances[utilization_class])

		for utilization_class in current_min_instances:
			pool[utilization_class][instance_type] = (
					current_min_instances[utilization_class])


def convert_to_yearly_estimated_hours(logged_hours, interval):
	"""Takes a min and max time and will convert to the amount of hours estimated
	for a year.

	example: If interval was 2 months, we want the yearly cost
	so this would convert the 2 months into 60 days and then
	would multiply all the hours in logs by 365.0 / 60 to get the yearly
	amount used.

	Args:
		logs: The hours that each utilization type and each instance of that util
			that has been calculated in a simulation.

		interval: The span of time (timedelta) that the all the job flows ran in.

	Mutates:
		logs: Will multiply all the hours used by each instance type by the
			conversion rate calculated.

	Returns: nothing
	"""
	days_per_year = 365.0
	conversion_rate = 365.0
	if interval.days is not 0:
		conversion_rate = (days_per_year /
			(interval.days + interval.seconds / (24.0 * 60 * 60)))
	for utilization_class in logged_hours:
		for machine in logged_hours[utilization_class]:
			logged_hours[utilization_class][machine] = (
				ceil(logged_hours[utilization_class][machine] * conversion_rate))

