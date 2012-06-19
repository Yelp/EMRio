# Inspect job flows and print out possibly useful information if the job flows
# can save money by switching to reserved instances and how many instances to
# buy and how much savings that would give.
#
#Usage:
#
#	python instance_tool.py > Statistics.txt
#	(for more options just use --help)
#
# Some useful definitions of variables used a lot are given below:
#
# 'pool'- This is a storage variable that holds the amount of instances
#		currently bought. Since there are multiple levels of
# utilization_class levels
#		and multiple instance types, the layout of pool is like this:
# pool = {
#	UTILIZATION_LEVEL: {
#		INSTANCE_NAME: INSTANCE_COUNT
#	}
# }
# UTILIZATION_LEVEL is a name that corresponds to
# utilization_class levels found here:
# 		http://aws.amazon.com/ec2/reserved-instances/#2
# INSTANCE_NAMES is the name that amazon uses for their instance types.
# INSTANCE_COUNT is how many instances are 'bought' for that simulation.
#
#
# logs- is the amount of hours that an instance type at a util level has run
#		in the span of the job flows. It is structured the same as pool except
#		INSTANCE_COUNT is hours.
#
#
# job_flows- This is a list of sorted and translated-job-dict-objects. Jobs are
#		based on the boto JobFlow object, except in dict form. Currently, we
#		only use start and end times, jobflowids, and instancegroups and
#		instancegroup objects. More info can be found here:
# 		boto.cloudhackers.com/en/latest/ref/emr.html#boto.emr.emrobject.JobFlow

import sys
from optparse import OptionParser

from ec2_cost import EC2
from simulate_jobs import Simulator
from graph_jobs import cost_graph, total_hours_graph
from job_filter import get_job_flows
from optimizer import convert_to_yearly_hours, Optimizer


# This is used for calculating on-demand usage.
EMPTY_INSTANCE_POOL = EC2.init_empty_reserve_pool()


def main(args):
	option_parser = make_option_parser()
	options, args = option_parser.parse_args(args)

	job_flows = get_job_flows(options)
	pool = get_best_instance_pool(options, job_flows)
	optimal_logged_hours, demand_logged_hours = simulate_job_flows(job_flows,
		pool)
	output_statistics(optimal_logged_hours, pool, demand_logged_hours)

	# If you specified a type of graph, display it here. Check options
	# for more details.
	if options.graph == 'cost':
		cost_graph(job_flows, pool)
	elif options.graph == 'total_usage':
		total_hours_graph(job_flows, pool)


def make_option_parser():
	usage = '%prog [options]'
	description = 'Print a giant report on EMR usage.'
	option_parser = OptionParser(usage=usage, description=description)
	option_parser.add_option(
		'-v', '--verbose', dest='verbose', default=False, action='store_true',
		help='print more messages to stderr')
	option_parser.add_option(
		'-q', '--quiet', dest='quiet', default=False, action='store_true',
		help="Don't log status messages; just print the report.")
	option_parser.add_option(
		'-c', '--conf-path', dest='conf_path', default=None,
		help='Path to alternate mrjob.conf file to read from')
	option_parser.add_option(
		'--no-conf', dest='conf_path', action='store_false',
		help="Don't load mrjob.conf even if it's available")
	option_parser.add_option(
		'--max-days-ago', dest='max_days_ago', type='float', default=None,
		help=('Max number of days ago to look at jobs. By default, we go back'
			'as far as EMR supports (currently about 2 months)'))
	option_parser.add_option(
		'--max-day', dest='max_days', type='string', default=None,
		help=('The first day to last looking in the job flows. If any job'
			'ends after this day, it is discarded (e.g.: --max-days 2012/05/07)'))
	option_parser.add_option(
		'--min-day', dest='min_days', type='string', default=None,
		help=('The first day to start looking in the job flows. If any job'
			'starts before this day, it is discarded (e.g.: --max-days 2012/05/07)')
		)
	option_parser.add_option(
		'-f', '--file', dest='file_inputs', type='string', default=None,
		help="Input a file that has job flows JSON encoded. The format is 1 job"
			"per line or comma separated jobs."
		)
	option_parser.add_option(
		'-o', '--optimized', dest='optimized_file', type='string', default=None,
		help=("Uses a previously saved optimized pool instead of calculating it from"
		" the job flows"))
	option_parser.add_option(
		'--save_optimized', dest='save', type='string', default=None,
		help='Save the optimized results so you dont calculate them multiple times')
	option_parser.add_option(
		'-g', '--graph', dest='graph', type='string', default='None',
		help='Load a graph of the job flows. Current graphs are: cost, total_usage')
	return option_parser


def get_best_instance_pool(options, job_flows):
	"""Returns the best instance flow based on the job_flows passed in or
	a file passed in by the user.

	Args:
		options: An OptionParser created from the arguments from the command line.

		job_flows: A list of dicts of jobs that have run over a period of time.

	Returns:
	"""
	if options.optimized_file:
		pool = read_optimal_instances(options.optimized_file)
	else:
		pool = Optimizer(job_flows).run()

	if options.save:
		write_optimal_instances(options.save, pool)
	return pool


def write_optimal_instances(filename, pool):
	"""Save optimal results since the data doesn't change too much for a given
	job flow.

	Args:
		filename: name of file.

		pool: A dict of optimal reserved instances to buy.

	Returns: Nothing
	"""
	f = open(filename, 'w')
	for utilization_class in pool:
		for machine in pool[utilization_class].keys():
			f.write("%s,%s,%d\n" %
					(utilization_class, machine, pool[utilization_class][machine]))
	f.close()


def read_optimal_instances(filename):
	"""Reads in a file of optimized instances instead of doing the simulation
	optimization which is slow.
	"""

	pool = EC2.init_empty_reserve_pool()
	f = open(filename, 'r')
	for line in f:
		utilization_class, machine, count = line.split(',')
		utilization_class = utilization_class
		pool[utilization_class][machine] = int(count)
	return pool


def simulate_job_flows(job_flows, pool):
	""" Gets the hours used in a simulation with the job flows and pool and
	the demand hours used as well.

	Returns:
		optimal_logged_hours: The amount of hours that each reserved instance
			used from the given job flow.

		demand_logged_hours: The amount of hours used per instance on just purely
			on demand instances, no reserved instances. Use this as a control group.
	"""
	job_flows_begin_time = min(job.get('startdatetime') for job in job_flows)
	job_flows_end_time = max(job.get('enddatetime') for job in job_flows)
	interval_job_flows = job_flows_end_time - job_flows_begin_time

	# Get the min and max times so that the yearly estimate can be made.
	optimal_simulator = Simulator(job_flows, pool)
	demand_simulator = Simulator(job_flows, EMPTY_INSTANCE_POOL)
	optimal_logged_hours = optimal_simulator.run()
	demand_logged_hours = demand_simulator.run()

	# Default_log is the log of hours used for purely on demand instances
	# with no reserved instances purchased for comparison later.
	convert_to_yearly_hours(demand_logged_hours, interval_job_flows)
	convert_to_yearly_hours(optimal_logged_hours, interval_job_flows)
	return optimal_logged_hours, demand_logged_hours


def output_statistics(log, pool, demand_log,):
	"""Once everything is calculated, output here"""

	optimized_cost = EC2.calculate_cost(log, pool)
	demand_cost = EC2.calculate_cost(demand_log, EMPTY_INSTANCE_POOL)

	for utilization_class in pool:
		print utilization_class, "Instance Pool Used ***************"
		for machine in pool[utilization_class]:
			print "\t%s: %d" % (machine, pool[utilization_class][machine])
		print ""

	for utilization_class in log:
		print utilization_class, "Hours Used **************"
		for machine in log[utilization_class]:
			print "\t%s: %d" % (machine, log[utilization_class][machine])

	print
	print "ENTIRELY ON DEMAND STATISTICS"
	print " Hours Used **************"
	for utilization_class in demand_log:
		for machine in demand_log[utilization_class]:
			print "\t%s: %d" % (machine, demand_log[utilization_class][machine])

	print "Cost difference:"
	print "Cost for Reserved Instance: ", optimized_cost
	print "Cost for all On-Demand: ", demand_cost
	print "Money Saved: ", (demand_cost - optimized_cost)


if __name__ == '__main__':
	main(sys.argv[1:])
