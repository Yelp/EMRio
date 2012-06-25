""" Graphing tools for EMRio.

This tool uses an observer to pull information from a simulation of jobs.
Once it has that information, it will use hours recorded and matplotlib to make
graphs from the job flows..
"""
import copy

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from config import EC2, TIMEZONE
from simulate_jobs import Simulator, SimulationObserver

COLORS = EC2.color_scheme()


def total_hours_graph(job_flows, pool):
	"""Graph the total hours used by reserved / ondemand
	instances over a period of time"""
	log_hours, hours = record_log_data(job_flows, pool)
	graph_over_time(log_hours, hours)


def cost_graph(job_flows, pool):
	"""This will graph the instances used and the type of
	instance used over time.
	"""
	used, hours = record_used_instances(job_flows, pool)
	graph_over_time(used, hours, job_flows)


def record_used_instances(job_flows, pool):
	"""Stores information regarding what instances were in the
	'used_pool' during the job simulation at all points of the
	simulation.
	"""
	used_per_hour = EC2.init_empty_all_instance_types()
	hour_graph = {}
	instance_simulator = Simulator(job_flows, pool)
	observer = SimulationObserver(hour_graph, used_per_hour)
	instance_simulator.attach_pool_use_observer(observer)
	instance_simulator.run()
	return used_per_hour, hour_graph


def record_log_data(job_flows, pool):
	"""This will set up the record information to graph total hours
	logged in a simulation over time.
	"""
	log_per_hour = EC2.init_empty_all_instance_types()
	hour_graph = {}
	log_simulator = Simulator(job_flows, pool)
	observer = SimulationObserver(hour_graph, log_per_hour)
	log_simulator.attach_log_hours_observer(observer)
	log_simulator.run()

	return  log_per_hour, hour_graph


def graph_over_time(logged_info, hours_line, job_flows,
	xlabel='Time job ran (in hours)',
	ylabel='Instances run'):
	"""Given some sort of data that changes over time, graph the
	data usage using this"""

	begin_time = min(job.get('startdatetime') for job in job_flows)
	end_time = max(job.get('enddatetime') for job in job_flows)
	
	# The graph looks better ending on a new day, so this changes it to a new day.
	if end_time.hour != 0:
		end_time = end_time.replace(hour=0, day=(end_time.day + 1))

	for instance_type in EC2.instance_types_in_pool(logged_info):
		# Locators / Formatters to pretty up the graph.
		hours = mdates.HourLocator(byhour=None, interval=1, tz=TIMEZONE)
		days = mdates.DayLocator(bymonthday=None, interval=1, tz=TIMEZONE)
		formatter = mdates.DateFormatter("%m/%d ", TIMEZONE)

		fig = plt.figure()
		fig.suptitle(instance_type)
		ax = fig.add_subplot(111)
		date_list = mdates.date2num(hours_line[instance_type])

		# Need to plot DEMAND -> HEAVY_UTIL since they are stacked which means
		# DEMAND will be the largest and needs to be drawn first so others draw over.
		all_utilization_classes = copy.deepcopy(EC2.ALL_PRIORITIES)
		all_utilization_classes.reverse()

		for utilization_class in all_utilization_classes:
			ax.plot(date_list, logged_info[utilization_class][instance_type], color='#000000')
			ax.plot(date_list[0], logged_info[utilization_class][instance_type][0],
				color=COLORS[utilization_class],
				label=utilization_class)
			ax.fill_between(date_list, logged_info[utilization_class][instance_type],
				color=COLORS[utilization_class],
				alpha=1.0)

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

