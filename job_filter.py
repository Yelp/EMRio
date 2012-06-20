"""The job handler does the following:
-translates the datetimes from unicode to datetime objects.
-will filter out max and min dates if specified in options
-will remove demunitive jobs that have no start or end dates.
d
"""
import datetime
import json
import pytz
# The user doesn't necessarily need mrjob, so we check to see if
# they have mrjob, if not, then we use the backup BOTO
import boto.exception
try:
	mrjob = True
	from mrjob.emr import EMRJobRunner
except ImportError:
	mrjob = None

	from boto.emr.connection import EmrConnection


def get_job_flows(options):
	"""This will check the options and use a file if provided or
	will get the job_flow data from amazon's cluster if no file
	is provided

	Args:
		options: An OptionParser object that has args stored in it.

	Returns:
		job_flows: A list of dicts of jobs that have run over a period of time.
	"""
	job_flows = []
	if(options.file_inputs):
		job_flows = handle_job_flows_file(options.file_inputs)
	else:
		job_flows = get_job_flow_objects(options)
	
	job_flows = no_date_filter(job_flows)
	job_flows = convert_dates(job_flows)
	job_flows = range_date_filter(job_flows, options.min_days, options.max_days)
	def sort_by_job_times(job1, job2):
		"""Sorting comparator for job_flow objects"""
		date1 = job1.get('startdatetime')
		date2 = job2.get('startdatetime')
		return_time = -1 if date1 < date2 else 1
		return return_time
	# sort job flows before running simulations.
	job_flows = sorted(job_flows, cmp=sort_by_job_times)
	print len(job_flows)
	for job in job_flows:
		print job.get('jobflowid')
	return job_flows


def convert_dates(job_flows):
	"""Converts the dates of all the jobs to the datetime object
	since they are originally in unicode strings
	Args:
		job: Current job being filtered.
		timezone: The timezone that we want to calculate everything in.

	Mutates:
		job.startdatetime: Changes from unicode to datetime.
		job.enddatetime: Changes from unicode to datetime

	Returns: Nothing.
	"""
	for job in job_flows:
		job['startdatetime'] = parse_date(job['startdatetime'])
		job['enddatetime'] = parse_date(job['enddatetime'])

	return job_flows


def no_date_filter(job_flows):
	"""Looks at a job and sees if it is missing a start or end date,
	which screws up simulations, so we remove them with this filter.

	Returns:
		boolean of whether the date is valid
	"""
	new_job_flows = []
	for job in job_flows:
		if job.get('startdatetime') and job.get('enddatetime'):
			new_job_flows.append(job)
	
	return new_job_flows


def range_date_filter(job_flows, min_days, max_days):
	"""If there is a min or max day, check to see if the job is within the bounds
	of the range, and remove any that are not.

	Returns:
		boolean of whether the job is within date range.
	"""
	new_job_flows = []
	if min_days:
		min_days = datetime.datetime.strptime(min_days, "%Y/%m/%d") 
		min_days = min_days.replace(tzinfo=pytz.utc)
	if max_days:
		max_days = datetime.datetime.strptime(max_days, "%Y/%m/%d") 
		max_days = max_days.replace(tzinfo=pytz.utc)
	for job  in job_flows:
		add_job = True
		if min_days and job['startdatetime'] < min_days:
			add_job = False
		if max_days and job['enddatetime'] > max_days:
			add_job = False
		if add_job:
			new_job_flows.append(job)
	return new_job_flows


def parse_date(str_date):
	current_date = datetime.datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
	current_date = current_date.replace(tzinfo=pytz.utc)
	return current_date


def handle_job_flows_file(filename):
	"""If you specify a file of job_flow objects, this function loads them. Will
	try comma-separated JSON objects and per-line objects before failing.
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


def get_job_flows_from_amazon(options):
	"""gets all the job flows from amazon and converts them into
	a dict for compatability with loading from a file
	"""
	now = datetime.datetime.utcnow()
	job_flows = get_job_flow_objects(options.conf_path, options.max_days_ago, now=now)
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


#### Code ported from mrjob.tools.emr.audit_usage #####
def get_job_flow_objects(conf_path, max_days_ago=None, now=None):
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
		created_after = now - datetime.timedelta(days=max_days_ago)

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
		created_before = datetime.datetime.utcnow() + datetime.timedelta(days=1)

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
			created_before = min_create_time + datetime.timedelta(seconds=1)
			# if someone managed to start 501 job flows in the same second,
			# they are still screwed (the EMR API only returns up to 500),
			# but this seems unlikely. :)
		else:
			if not created_before:
				created_before = datetime.utcnow()
			created_before -= datetime.timedelta(weeks=2)
	return all_job_flows


