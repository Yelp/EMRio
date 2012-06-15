""" This tool uses a logger to pull information from a simulation of jobs.
Once it has that information, it will use that and matplotlib to make
a graph on the type of graph you specified in the options of instance_tool.
"""
import matplotlib.pyplot as plt
import copy
import matplotlib.dates as mdates
from pytz import timezone

from simulate_jobs import simulate_job_flows
from ec2_cost import EC2
TIME = timezone('US/Alaska')
COLORS = EC2.color_scheme()


def total_hours_graph(job_flows, pool):
	"""Graph the total hours used by reserved / ondemand
	instances over a period of time"""
	log_hours, hours = record_log_data(job_flows,
		pool)
	begin_time = min(job.get('startdatetime') for job in job_flows)
	end_time = max(job.get('enddatetime') for job in job_flows)
	if end_time.hour != 0:
		end_time = end_time.replace(hour=0, day=(end_time.day + 1))
	graph_over_time(log_hours, hours, begin_time, end_time)


def cost_graph(job_flows, pool):
	"""Need a better name, but this will graph the instances
	used and the type of instance used over time.
	"""
	used, hours = record_used_instances(job_flows, pool)
	begin_time = min(job.get('startdatetime') for job in job_flows)
	end_time = max(job.get('enddatetime') for job in job_flows)
	if end_time.hour != 0:
		end_time = end_time.replace(hour=0, day=(end_time.day + 1))
	graph_over_time(used, hours, begin_time, end_time)
	print pool


def record_used_instances(job_flows, pool):
	"""Stores information reguarding what instances were in the
	'used_pool' during the job simulation at all points of the
	simulation.
	"""
	used_per_hour = EC2.init_empty_all_instance_types()
	hour_graph = {}

	def instance_usage_logger(time, node_type, job, _, used):
		"""Logs all instance hour useage for each time node in the
		priority queue. This will just append the pool usage at that point
		in the simulator.


		Example: Let's say we have only 2 events at time 1 and time 3.
		This will record the used_pool at time 1 before any action is
		taken. Let's say in the event at time 1, it was a START event,
		so after time 1, there will be X instances used to accomodate for
		the new job, which will be recorded. At time 3, it is the end event
		for the job, so after time 3, the used pool will be back at 0.
		On a basic level, the lists will look something like this:
		used[utilization][instance_type]: [0  5  5  0]
		time[instance_type]:              [1  1  3  3]

		"""

		for instance in job.get('instancegroups'):
			i_type = instance.get('instancetype')
			# Add the time this event occurred at.
			current_time_line = hour_graph.get(i_type, [])
			current_time_line.append(time)
			hour_graph[i_type] = current_time_line

			# Calculate the total at each point, then increasingly
			# Add the next util group onto the previous one.
			total = 0
			for util in EC2.ALL_PRIORITIES:
				if i_type not in used_per_hour[util]:
					used_per_hour[util][i_type] = []
				total += used[util].get(i_type, 0)
				used_per_hour[util][i_type].append(total)

	simulate_job_flows(job_flows, pool, logger=instance_usage_logger)
	return used_per_hour, hour_graph


def record_log_data(job_flows, pool):
	"""This will set up the record information to graph total hours
	logged in a simulation over time.
	"""
	log_per_hour = EC2.init_empty_all_instance_types()
	hour_graph = {}

	def instance_hour_logger(time, node_type, job, log, _):
		"""Logs all instance hour useage for each time node in the
		priority queue. The logger is called twice in a single event.
		So this records the state of log pool before and after some event"""

		for instance in job.get('instancegroups'):
			i_type = instance.get('instancetype')

			# Add the time this event occurred at.
			current_time_line = hour_graph.get(i_type, [])
			current_time_line.append(time)
			hour_graph[i_type] = current_time_line

			# Grab the total usage logs for each util type.
			# It is a cumulative total to make a stacked graph which looks cooler.
			total = 0
			for util in EC2.ALL_PRIORITIES:
				if i_type not in log_per_hour[util]:
					log_per_hour[util][i_type] = []
				total += log[util].get(i_type, 0)
				log_per_hour[util][i_type].append(total)

	simulate_job_flows(job_flows, pool, logger=instance_hour_logger)
	return  log_per_hour, hour_graph


def graph_over_time(logged_info, hours_line, begin_time, end_time,
	xlabel='Time job ran (in hours)', ylabel='Instances run'):
	"""Given some sort of data that changes over time, graph the
	data usage using this"""

	for i_type in EC2.instance_types_in_pool(logged_info):
		# Locators / Formatters to pretty up the graph.
		hours = mdates.HourLocator(byhour=None, interval=1, tz=TIME)
		days = mdates.DayLocator(bymonthday=None, interval=1, tz=TIME)
		formatter = mdates.DateFormatter("%m/%d ", TIME)

		fig = plt.figure()
		fig.suptitle(i_type)
		ax = fig.add_subplot(111)
		date_list = mdates.date2num(hours_line[i_type])

		# Need to plot DEMAND -> HEAVY_UTIL since they are stacked which means
		# DEMAND will be the largest and needs to be drawn first so others draw over.
		iterator = copy.deepcopy(EC2.ALL_PRIORITIES)
		iterator.reverse()

		for util in iterator:
			ax.plot(date_list, logged_info[util][i_type], color='#000000')
			ax.plot(date_list[0], logged_info[util][i_type][0],
				color=COLORS[util], label=util)
			ax.fill_between(date_list, logged_info[util][i_type],
				color=COLORS[util], alpha=1.0,)

		ax.xaxis.set_major_locator(days)
		ax.xaxis.set_major_formatter(formatter)
		ax.xaxis.set_minor_locator(hours)

		ax.set_xlabel(xlabel)
		ax.set_ylabel(ylabel)
		ax.set_xlim(begin_time, end_time)
		ax.grid(True)
		ax.legend()
		plt.xticks(rotation='vertical')
	plt.show()
