from iso8601 import parse_date
import datetime
from pytz import timezone


# The Job Filter does the following:
# -translates the datetimes from unicode to datetime objects.
# -will filter out max and min dates if specified in options
# -will remove demunitive jobs that have no start or end dates.
#
# It can be added onto easily by just adding more functions and then
# adding those functions to self.filters list which will apply the function
# to all jobs.
class JobFilter:

	def __init__(self, options):
		self.filters = [self.no_date_filter]

		self.time_tz = timezone('US/Pacific')  # Change this for a different tz
		midnight = datetime.time(0, 0, 0, 0, self.time_tz)

		# These options will take the year, month and day while sticking the time of
		# midnight to that date. It is a workaround to deal with naive/complex tz
		if options.min_days:
			self.min_days = datetime.datetime.strptime(options.min_days, "%Y/%m/%d")
			self.min_days = datetime.datetime.combine(self.min_days.date(), midnight)
			print self.min_days.ctime()
		if options.max_days:
			self.max_days = datetime.datetime.strptime(options.max_days, "%Y/%m/%d")
			self.max_days = datetime.datetime.combine(self.max_days.date(), midnight)
		if options.max_days or options.min_days:
			self.filters.append(self.range_date_filter)

	def filter_jobs(self, job_flows):
		"""The main function to call for job filter. This will remove any jobs
		that are not within the filter-requirements specified by each function
		added to the filter.

		returns:  job_flows that has have only relevant jobs in it.
		"""
		new_jobs = []
		for job in job_flows:
			accept = True
			self.convert_dates(job, self.time_tz)
			for cur_filter in self.filters:
				accept = cur_filter(job)
				if accept is not True:
					break
			if accept is True:
				new_jobs.append(job)
		return new_jobs

	def convert_dates(self, job, timezone):
		"""Converts the dates of all the jobs to the datetime object
		since they are originally in unicode strings

		returns: Nothing.
		mutates: job startdatetime and job enddatetime
		"""
		start_date = job.get('startdatetime', None)
		end_date = job.get('enddatetime', None)
		if start_date != None:
			job['startdatetime'] = parse_date(start_date)
			job['startdatetime'] = job['startdatetime'].astimezone(timezone)
		else:
			job['startdatetime'] = None

		if end_date != None:
			job['enddatetime'] = parse_date(end_date)
			job['enddatetime'] = job['enddatetime'].astimezone(timezone)
		else:
			job['enddatetime'] = None

	def no_date_filter(self, job):
		"""Looks at a job and sees if it is missing a start or end date,
		which screws up simulations, so we remove them with this filter.

		returns: boolean of whether the date is valid
		"""
		if job.get('startdatetime') is None or job.get('enddatetime') is None:
			return False
		else:
			return True

	def range_date_filter(self, job):
		"""If there is a min or max day, check to see if the job is within the bounds
		of the range, and remove any that are not.

		returns: boolean of whether the job is within date range.
		"""
		if getattr(self, 'min_days', None):
			if job['startdatetime'] < self.min_days:
				return False
		if getattr(self, 'max_days', None):
			if job['enddatetime'] > self.max_days:
				return False
		return True
