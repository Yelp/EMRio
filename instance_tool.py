# Inspect job flows and print out possibly useful information if the job flows
# can save money by switching to reserved instances and how many instances to
# buy and how much savings that would give.
#
#Usage:
#
#	python instance_tool.py > Statistics.txt
#
#
# Some useful definitions of variables used a lot are given below:
#
# 'pool'- This is a storage variable that holds the amount of instances
#		currently bought. Since there are multiple levels of util levels
#		and multiple instance types, the layout of pool is like this:
# pool = {
#	UTILIZATION_LEVEL: {
#		INSTANCE_NAME: INSTANCE_COUNT
#	}
# }
# UTILIZATION_LEVEL is an int that corresponds to util levels found here:
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

import datetime
from datetime import timedelta
from optparse import OptionParser
import json
import sys

from iso8601 import parse_date
import boto.exception
try:
	mrjob = True
	from mrjob.emr import EMRJobRunner
except ImportError:
	mrjob = False

	from boto.emr.connection import EmrConnection


from ec2_cost import EC2
from simulate_jobs import simulate_job_flows
from graph_jobs import cost_graph, total_hours_graph
from job_filter import JobFilter
from optimizer import optimize_instance_pool, convert_to_yearly_hours


# This is used for calculating on-demand usage.
EMPTY_INSTANCE_POOL = EC2.init_empty_reserve_pool()


def main(args):
	option_parser = make_option_parser()
	options, args = option_parser.parse_args(args)

	if args:
		option_parser.error('takes no arguments')

	job_flows = job_flow_handler(options)

	# Get the min and max times so that the yearly estimate can be made.
	job_flows_begin_time = min(job.get('startdatetime') for job in job_flows)
	job_flows_end_time = max(job.get('enddatetime') for job in job_flows)
	interval_job_flows = job_flows_end_time - job_flows_begin_time

	if options.optimized_file:
		pool = read_optimal_instances(options.optimized_file)
	else:
		pool = optimize_instance_pool(job_flows, interval_job_flows)

	if options.save is not None:
		write_optimal_instances(options.save, pool)
	logs = simulate_job_flows(job_flows, pool)

	# Default_log is the log of hours used for purely on demand instances
	# with no reserved instances purchased for comparison later.
	default_log = simulate_job_flows(job_flows, EMPTY_INSTANCE_POOL)
	convert_to_yearly_hours(default_log, interval_job_flows)
	convert_to_yearly_hours(logs, interval_job_flows)

	output_statistics(logs, pool, default_log)

	# Graph stuff if specified.
	if options.graph == 'cost':
		cost_graph(job_flows, pool)
	elif options.graph == 'total_usage':
		total_hours_graph(job_flows, pool)


def job_flow_handler(options):
	"""This will check the options and use a file if provided or
	will get the job_flow data from amazon's cluster if no file
	is provided

	returns job_flows
	"""
	job_filter = JobFilter(options)
	job_flows = []
	if(options.file_inputs):
		job_flows = handle_job_flows_file(options.file_inputs)
	else:
		job_flows = get_job_flows_amazon(options)
	job_flows = job_filter.filter_jobs(job_flows)

	# sort job flows before running simulations.
	job_flows = sorted(job_flows, cmp=sort_by_job_times)
	return job_flows


def write_optimal_instances(filename, pool):
	"""Save optimal results since the data doesn't change too much for a given
	job flow."""
	f = open(filename, 'w')
	for util in pool:
		for machine in pool[util].keys():
			f.write("%s,%s,%d\n" % (util, machine, pool[util][machine]))
	f.close()


def read_optimal_instances(filename):
	"""Reads in a file if it is provided by from the command line
	instead of doing the simulation optimization which is slow.
	"""
	pool = EC2.init_empty_reserve_pool()
	f = open(filename, 'r')
	for line in f:
		util, machine, count = line.split(',')
		util = util
		pool[util][machine] = int(count)
	return pool


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
		'-g', '--graph', dest='save', type='string', default=None,
		help='Load a graph of the job flows. Current graphs are: cost, total_usage')
	return option_parser


def handle_job_flows_file(filename):
	"""If you specify a file of job_flow objects, this
	function loads them. Will try comma-separated JSON
	objects and per-line objects before failing.

	return job_flows
	"""
	try:
		current_file = open(filename, 'r')
		contents = current_file.read().rstrip('\n')[:-1]
		job_flows = json.loads(contents)
		return job_flows
	except ValueError:
		pass  # Might be a different format..
	job_flows = []
	current_file = open(filename, 'r')
	for line in current_file.readlines():
		job_flows.append(json.loads(line))
	return job_flows


def get_job_flows_amazon(options):
	"""gets all the job flows from amazon and converts them into
	a dict for compatability with loading from a file

	returns: job_flows
	"""
	now = datetime.datetime.utcnow()
	job_flows = get_job_flows(options.conf_path, options.max_days_ago, now=now)
	dict_job_flows = []
	for job in job_flows:
		job_dict = job.__dict__
		new_list = []
		for instance in job.instancegroups:
			new_list.append(instance.__dict__)
		job_dict['instancegroups'] = new_list
		dict_job_flows.append(job_dict)
	job_flows = dict_job_flows
	return job_flows


def output_statistics(log, pool, default_log,):
	"""Once everything is calculated, output here

	returns: nothing
	"""
	optimized_cost = EC2.calculate_cost(log, pool)
	demand_cost = EC2.calculate_cost(default_log, EMPTY_INSTANCE_POOL)

	for util in pool:
		print util, "Instance Pool Used ***************"
		for machine in pool[util]:
			print "\t%s: %d" % (machine, pool[util][machine])
		print ""

	for util in log:
		print util, "Hours Used **************"
		for machine in log[util]:
			print "\t%s: %d" % (machine, log[util][machine])

	print
	print "ENTIRELY ON DEMAND STATISTICS"
	print " Hours Used **************"
	for util in default_log:
		for machine in default_log[util]:
			print "\t%s: %d" % (machine, default_log[util][machine])

	print "Cost difference:"
	print "Cost for Reserved Instance: ", optimized_cost
	print "Cost for all On-Demand: ", demand_cost
	print "Money Saved: ", (demand_cost - optimized_cost)


def sort_by_job_times(job1, job2):
	"""Sorting comparator for job_flow objects"""
	date1 = job1.get('startdatetime')
	date2 = job2.get('startdatetime')
	return_time = -1 if date1 < date2 else 1
	return return_time


#### Stuff taken from mrjob.tools.emr.audit_usage #####
def get_job_flows(conf_path, max_days_ago=None, now=None):
	"""Get relevant job flow information from EMR.

	:param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from, or
						``False`` to ignore all config files.
	:param float max_days_ago: If set, don't fetch job flows created longer
								than this many days ago.
	:param now: the current UTC time, as a :py:class:`datetime.datetime`.
				Defaults to the current time.
	"""
	if now is None:
		now = datetime.utcnow()
	emr_conn = None
	if mrjob:
		emr_conn = EMRJobRunner(conf_path=conf_path).make_emr_conn()
	else:
		emr_conn = EmrConnection()
	# if --max-days-ago is set, only look at recent jobs
	created_after = None
	if max_days_ago is not None:
		created_after = now - timedelta(days=max_days_ago)

	return describe_all_job_flows(emr_conn, created_after=created_after)


def describe_all_job_flows(emr_conn, states=None, jobflow_ids=None,
							created_after=None, created_before=None):
	"""Iteratively call ``EmrConnection.describe_job_flows()`` until we really
	get all the available job flow information. Currently, 2 months of data
	is available through the EMR API.

	This is a way of getting around the limits of the API, both on number
	of job flows returned, and how far back in time we can go.

	:type states: list
	:param states: A list of strings with job flow states wanted
	:type jobflow_ids: list
	:param jobflow_ids: A list of job flow IDs
	:type created_after: datetime
	:param created_after: Bound on job flow creation time
	:type created_before: datetime
	:param created_before: Bound on job flow creation time
	"""
	all_job_flows = []
	ids_seen = set()

	if not (states or jobflow_ids or created_after or created_before):
		created_before = datetime.datetime.utcnow() + timedelta(days=1)

	while True:
		if created_before and created_after and created_before < created_after:
			break

		try:
			results = emr_conn.describe_jobflows(
				states=states, jobflow_ids=jobflow_ids,
				created_after=created_after, created_before=created_before)
		except boto.exception.BotoServerError, ex:
			if 'ValidationError' in ex.body:

				break
			else:
				raise

		# don't count the same job flow twice
		job_flows = [jf for jf in results if jf.jobflowid not in ids_seen]

		all_job_flows.extend(job_flows)
		ids_seen.update(jf.jobflowid for jf in job_flows)

		if job_flows:
			# set created_before to be just after the start time of
			# the first job returned, to deal with job flows started
			# in the same second
			min_create_time = min(parse_date(jf.creationdatetime)
									for jf in job_flows)
			created_before = min_create_time + timedelta(seconds=1)
			# if someone managed to start 501 job flows in the same second,
			# they are still screwed (the EMR API only returns up to 500),
			# but this seems unlikely. :)
		else:
			if not created_before:
				created_before = datetime.utcnow()
			created_before -= timedelta(weeks=2)
	return all_job_flows

if __name__ == '__main__':
	main(sys.argv[1:])
