"""The optimizer module holds all the functions relating to creating the
best instance pool that yields the least cost over an interval of time.
it takes a job flow and runs multiple simulations on the same data with
different reserved instance pool configurations. Optimizer takes into
account different utilization levels, different instance types and also
accounts for the weird HEAVY UTILIZATION payment cost (in EC2 class).

Some useful definitions of variables used a lot are given below:

'pool'- This is a storage variable that holds the amount of instances
		currently bought. Since there are multiple levels of util levels
		and multiple instance types, the layout of pool is like this:
pool = {
	UTILIZATION_LEVEL: {
		INSTANCE_NAME: INSTANCE_COUNT
	}
}
UTILIZATION_LEVEL is an int that corresponds to util levels found here:
		http://aws.amazon.com/ec2/reserved-instances/#2
INSTANCE_NAMES is the name that amazon uses for their instance types.
INSTANCE_COUNT is how many instances are 'bought' for that simulation.

logs- is the amount of hours that an instance type at a util level has run
		in the span of the job flows. It is structured the same as pool except
		INSTANCE_COUNT is hours.

job_flows- This is a list of sorted translated-job-dict-objects. Jobs are
		based on the boto JobFlow object, except in dict form. Currently, we
		only use start and end times, jobflowids, and instancegroups and
		instancegroup objects. More info can be found here:
		boto.cloudhackers.com/en/latest/ref/emr.html#boto.emr.emrobject.JobFlow
"""
from ec2_cost import EC2
from simulate_jobs import simulate_job_flows
from math import ceil


def optimize_instance_pool(job_flows, job_flows_interval=None):
	"""Take all the max_instance counts, then use that to combinatorically
	find the most cost efficient instance cost
	job_flows_interval is the interval of time that all the job flows ran.
		If none, then it will be calculated in the function.

	returns: dict of best pool of instances to be used.
	"""

	optimized_pool = EC2.init_empty_reserve_pool()
	if job_flows_interval is None:
		min_time = min(job.get('startdatetime') for job in job_flows)
		max_time = max(job.get('enddatetime') for job in job_flows)
		job_flows_interval = max_time - min_time

	# Zero-ing the instances just makes it so the optimized pool
	# knows all the instance_types the job flows use beforehand.
	zero_instance_types(job_flows, optimized_pool)
	for instance in EC2.instance_types_in_pool(optimized_pool):
		brute_force_optimize(instance, job_flows, optimized_pool, job_flows_interval)
	return optimized_pool


def brute_force_optimize(instance_type, job_flows, pool, job_flows_interval):
	"""The brute force approach will take a single instance type and optimize the
	instance pool for it. By using the job_flows in simulations.
	returns: nothing
	mutates: pool
	"""
	previous_cost = float('inf')
	current_min_cost = float("inf")
	current_cost = float('inf')
	current_min_instances = EC2.init_reserve_counts()
	# Calculate the default cost first.
	logs = simulate_job_flows(job_flows, pool)
	convert_to_yearly_hours(logs, job_flows_interval)
	current_min_cost = EC2.calculate_cost(logs, pool)
	current_cost = current_min_cost
	# Since there is only 1 best min value, any time adding
	# one more instance adds more to the cost, we know that the
	# previous instance was the best, so stop there.
	while previous_cost >= current_cost:
		current_simulation_costs = EC2.init_reserve_counts()
		for util in current_simulation_costs:
			current_simulation_costs[util] = float('inf')

		# Add a single instance to each utilization type, and record the costs.
		# whichever is the minimum, and choose it.
		for util in pool:
			# Reset the min instances to the best values.
			for current_util in pool:
				pool[current_util][instance_type] = current_min_instances[current_util]
			pool[util][instance_type] = current_min_instances[util] + 1
			logs = simulate_job_flows(job_flows, pool)
			convert_to_yearly_hours(logs, job_flows_interval)
			current_simulation_costs[util] = EC2.calculate_cost(logs, pool)
		previous_cost = current_cost
		current_cost = min(current_simulation_costs.values())
		min_util_level = None
		for util in current_simulation_costs:
			if current_simulation_costs[util] == current_cost:
				min_util_level = util

		# Record the new cost, then check to see if adding one instance is better
		# If it is not, then break from the loop, since adding more will be worst.
		if min(current_cost, current_min_cost) != current_min_cost or (
		current_cost == current_min_cost):

			current_min_cost = current_cost
			current_min_instances[min_util_level] += 1
		# Reset to best instance pool.
		for current_util in pool:
			pool[current_util][instance_type] = current_min_instances[util]

	for util in current_min_instances:
		pool[util][instance_type] = current_min_instances[util]


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
		job_flows: A filtered list of job dictionaries.

		pool: A dict of utilization level dictionaries with nothing in them.

	Mutates:
		pool: for each utilization type, it fills in all the instance_types
			that any job uses.
	Returns: Nothing
	"""
	for job in job_flows:
		for instance in job.get('instancegroups'):
			i_type = instance.get('instancetype')
			for util in pool.keys():
				pool[util][i_type] = 0


def convert_to_yearly_hours(logs, interval):
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
	for util in logs:
		for machine in logs[util]:
			logs[util][machine] = ceil(logs[util][machine] * conversion_rate)
