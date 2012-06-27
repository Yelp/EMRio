"""Inspect job flows to predict optimal pool of reserved instances that
minimizes the cost.

This module will take job flows from wherever you specify, and use them to
approximate the amount of reserved instances you should buy and how much money
you will save by doing so. 

If you are looking for instructions to run the program, look at the
readme in the root EMRio folder.
"""
import sys
import logging
from optparse import OptionParser

from config import EC2
from graph_jobs import instance_usage_graph
from graph_jobs import total_hours_graph
from job_handler import get_job_flows
from optimizer import convert_to_yearly_estimated_hours
from optimizer import Optimizer
from simulate_jobs import Simulator

# This is used for calculating on-demand usage.
EMPTY_INSTANCE_POOL = EC2.init_empty_reserve_pool()


def main(args):
	option_parser = make_option_parser()
	options, args = option_parser.parse_args(args)
	logging.basicConfig(level=logging.INFO)
	if options.verbose:
		logging.basicConfig(level=logging.DEBUG)
	logging.debug('Getting job flows...')
	job_flows = get_job_flows(options)

	logging.info('Finding optimal instance pool (this may take a minute or two)...')
	pool = get_best_instance_pool(job_flows, options.optimized_file, options.save)
	optimal_logged_hours, demand_logged_hours = simulate_job_flows(job_flows,
		pool)
	output_statistics(optimal_logged_hours, pool, demand_logged_hours)

	logging.debug('Making graphs...')
	if options.graph == 'instance_usage':
		instance_usage_graph(job_flows, pool)
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
		help='Load a graph of the job flows. Current graphs are: instance_usage, total_usage')
	return option_parser


def get_best_instance_pool(job_flows, optimized_filename, save_filename):
	"""Returns the best instance flow based on the job_flows passed in or
	a file passed in by the user.

	Args:
		optimized_filename: the name of a file with the optimal instances in it.
			if this is None, then get the data from Amazon.

		save_filename: the name of a file to save the calculated results to. 
			If none, don't save the file.

		job_flows: A list of jobs flow dictionary objects.

	Returns:
		pool of best optimal instances.
	"""
	if optimized_filename:
		pool = read_optimal_instances(optimized_filename)
	else:
		pool = Optimizer(job_flows).run()

	if save_filename:
		write_optimal_instances(save_filename, pool)
	return pool


def write_optimal_instances(filename, pool):
	"""Save optimal pool results.

	Format for saving is:
	UTILIZATION_CLASS,INSTANCE_NAME,INSTANCE_COUNT

	An example saved instance file is in the tests folder.
	Args:
		filename: name of the file to save the pool to.

		pool: A dict of reserved instances to buy.
	"""
	with open(filename, 'w') as f:
		for utilization_class in pool:
			for machine in pool[utilization_class].keys():
				f.write("%s,%s,%d\n" %
				(utilization_class, machine, pool[utilization_class][machine]))


def read_optimal_instances(filename):
	"""Reads the file name provided and uses it to create an optimal instance
	pool.
	
	If you want to see the format of these files, check the tests folder.

	Returns:
		pool: The utilization class and instances read from the file specified.
	"""

	pool = EC2.init_empty_reserve_pool()
	with open(filename, 'r') as f:
		for line in f:
			utilization_class, machine, count = line.split(',')
			utilization_class = utilization_class
			pool[utilization_class][machine] = int(count)
		return pool


def simulate_job_flows(job_flows, pool):
	"""Simulates the job flows using the pool, and will also simulate pure
	on-demand hours with no pool and return both.

	Returns:
		optimal_logged_hours: The amount of hours that each reserved instance
			used from the given job flow.

		demand_logged_hours: The amount of hours used per instance on just purely
			on demand instances, no reserved instances. Use this as a control group.
	"""
	job_flows_begin_time = min(job.get('startdatetime') for job in job_flows)
	job_flows_end_time = max(job.get('enddatetime') for job in job_flows)
	interval_job_flows = job_flows_end_time - job_flows_begin_time

	optimal_simulator = Simulator(job_flows, pool)
	demand_simulator = Simulator(job_flows, EMPTY_INSTANCE_POOL)
	optimal_logged_hours = optimal_simulator.run()
	demand_logged_hours = demand_simulator.run()

	convert_to_yearly_estimated_hours(demand_logged_hours, interval_job_flows)
	convert_to_yearly_estimated_hours(optimal_logged_hours, interval_job_flows)
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
	print "Cost for Reserved Instance: $%.2f " % optimized_cost
	print "Cost for all On-Demand: $%.2f" % demand_cost
	print "Money Saved: $%.2f" % (demand_cost - optimized_cost)


if __name__ == '__main__':
	main(sys.argv[1:])
