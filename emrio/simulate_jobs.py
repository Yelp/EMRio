""" The Simulate Jobs module uses a job flow history and outputs a log of hours
for each instance type and utilization class used.

This module is used to simulate job flows over a period of time and give back
the hours used for each instance type by the job flows. You also pass in a
reserved instance pool which the simulator will use to allocate jobs to when
it can, according to Amazon's guidelines on reserved instances. It will
record the individual utilization types of reserved instances so you can get
detailed hour usage on the reserved instances.

Some useful variables that are used frequently in Simulator:

logged_hours- is the amount of hours that an instance type at a util level has run
		in the span of the job flows. It is structured the same as pool except
		INSTANCE_COUNT is hours ran.

jobs_running (sometimes called jobs) - This is a dict of currently running
		jobs in the simulator. This is how it is structured:

jobs_running = {
	JOB_ID: {
		UTILIZATION_LEVEL: {
			INSTANCE_TYPE: INSTANCE_COUNT
		}
	}
}
JOB_ID is the id for the job running. The rest is setup like logs and pool.
		It is used to keep track of what the job is currently using in instances.
"""

import datetime
from config import EC2
from heapq import heapify, heappop

# If there are events happening at the same time in the priority queue, START
# needs to occur later than END, so the const numbers are priority encoded
# where end has the highest precedence, then LOG and finally START.
START = 2
LOG = 1
END = 0


class Simulator:

	def __init__(self, job_flows, pool):
		self.pool = pool
		self.job_flows = job_flows
		self.log_observers = []
		self.use_pool_observers = []

	def run(self):
		"""Will simulate a job flow using a reserved instance pool.

		Record a job flow history as if it was on reserved instances, and then log
		results of how many hours the reserved instances were used.

		Args:
			job_flows: list of jobs (which are dicts) which run the jobs in the list
				with reserved instances.

			instance_pool: dict of util constants (holding dict values) to tell the
				simulator the reserved instances you "bought"

			logger: a function that logs data each pass of the simulator. This way
				each pass of the simulator can be recorded instead of cumulative sums
				of logs.

		Returns:
			log: A dict that holds the cumulative hours ran on all instance types and
				utilization levels.
		"""
		# Setup the queue, state variables and logger.
		job_event_timeline = self.setup_job_event_timeline()
		logged_hours = EC2.init_empty_all_instance_types()
		# The pool used is the amount of instances that are currently in
		# use by the simulator. available instances = pool - used.
		pool_used = EC2.init_empty_all_instance_types()

		jobs_running = {}

		# Start simulating events.
		for time, event_type, job in [heappop(job_event_timeline)
					for i in range(len(job_event_timeline))]:
			job_id = job.get('jobflowid')

			# Logger is used for recording more information as the simulator runs
			# by passing in a logger function, you can use closure to access other
			# variables and log the information you want (example: graphs)
			self.notify_observers(time, event_type, job, logged_hours, pool_used)
			if event_type is START:
				self.allocate_job(jobs_running, pool_used, job)

			elif event_type is LOG:
				self.log_hours(logged_hours, jobs_running, job_id)

				# Due to billing switching if instances can run on reserve,
				# we must rearrange the instances each billing hour.
				self.rearrange_instances(jobs_running, pool_used, job)

			elif event_type is END:
				self.log_hours(logged_hours, jobs_running, job_id)
				self.remove_job(jobs_running, pool_used, job)

			self.notify_observers(time, event_type, job, logged_hours, pool_used)
		return logged_hours

	def setup_job_event_timeline(self):
		"""Sets up node events for the simulator.

		Create a priority queue where the events are the
		start and end times of jobs, with intermediate log-hour
		events for switching to reserved instances and logging hours of jobs.

		NOTE: The reason for LOG events, is because each hour a job runs, it has a
		chance that a reserved instance has opened up. If it has opened up, then
		it needs to switch billing to that open instance. One cannot calculate when
		this happens with only just START and END events.

		Returns:
			event_timeline: a priority queue of event tuples:
				(TIME, EVENT_TYPE, job)
				TIME -- datetime the event occurs at.
				EVENT_TYPE -- START, LOG or END.
		"""
		job_event_timeline = []
		for job in self.job_flows:
			start_time = job.get('startdatetime')
			hour_increment = start_time + datetime.timedelta(0, 3600)
			end_time = job.get('enddatetime')

			# This creates intermediate nodes for logging hours.
			while hour_increment < end_time:
				medium_node = (hour_increment, LOG, job)
				job_event_timeline.append(medium_node)
				hour_increment += datetime.timedelta(0, 3600)

			# Create nodes and add them to the heap.
			start_node = (start_time, START, job)
			end_node = (end_time, END, job)
			job_event_timeline.append(start_node)
			job_event_timeline.append(end_node)

		heapify(job_event_timeline)
		return job_event_timeline

	def attach_log_hours_observer(self, observer):
		self.log_observers.append(observer)
	
	def attach_pool_use_observer(self, observer):
		self.use_pool_observers.append(observer)

	def notify_observers(self, time, event_type, job, logged_hours, pool_used):
		"""Sends information to observers that are attached to the
		simulator.
		Args:
			time: Time the event occurred at.

			event_type: START, LOG, or END event.

			job: The current job that the event occurred for.

			logged_hours: The sum of the logged hours up to this point.

			pool_used: The current instances and their types used at this point in time.
		"""

		for observer in self.use_pool_observers:
			observer.update(time, event_type, job, pool_used)

		for observer in self.log_observers:
			observer.update(time, event_type, job, logged_hours)

	def log_hours(self, logged_hours, jobs, job_id):
		"""Will add the hours of the specified job running to the logs.

		job_id -- type: String
			use: To index into jobs to get the amount of instances it is using.
		"""
		for utilization_class in jobs.get(job_id, None):
			for instance_type in jobs[job_id].get(utilization_class, None):
				logged_hours[utilization_class][instance_type] = (
						logged_hours[utilization_class].get(instance_type, 0)
						+ jobs[job_id][utilization_class][instance_type])

	def allocate_job(self, jobs, pool_used, job):
		"""Puts a job in job_running and allocates necessary instances to it.

		When a job event is fired, or reallocation is required,
		this function will go through the instance pool and try to find an open
		utilization starting from the best priority and working its way down. It
		fills in as many instances it can for each utilization level.

		Args:
			jobs: a dict of jobs running and their instance usage. Used to
				put the current job into it.

			pool_used: a dict of current instances in use. Use to allocate jobs.
		"""
		job_id = job.get('jobflowid')

		# A small function that will choose the amount of instances used.
		# If the job needs more instances than the pool has, choose have.
		use_space = lambda need, have: need if need < have else have
		jobs[job_id] = {}
		for instance in job.get('instancegroups', []):
			instance_type = instance.get('instancetype')
			instances_needed = int(instance.get('instancerequestcount', 0))

			for utilization_class in EC2.ALL_UTILIZATION_PRIORITIES:
				current_use = pool_used[utilization_class].get(instance_type, 0)
				space_left = self._calculate_space_left(current_use,
						utilization_class, instance_type)

				# If there is space, use some of the instance pool, otherwise continue.
				if space_left > 0:
					instances_utilized = use_space(instances_needed, space_left)
					pool_used[utilization_class][instance_type] = (current_use +
							instances_utilized)
					instances_needed -= instances_utilized

					# Record job data for use in logging later.
					jobs[job_id][utilization_class] = jobs[job_id].get(
							utilization_class, {})
					jobs[job_id][utilization_class][instance_type] = instances_utilized

				if instances_needed == 0:
					break

	def remove_job(self, jobs, pool_used, job):
		"""Removes a job when it has ended from the current job flow.

		Args:
			jobs: a dict of currently running jobs.

			pool_used: A dict of all the instances in use.

			job: The current job that is being deallocated.

		Mutates:
			jobs: Removes the job from the currently running jobs.
			pool_used: Removes instances that the job was using, so they are
				now free.
		"""
		job_id = job.get('jobflowid')

		# Remove all the pool used by the instance then delete the job.
		for utilization_class in jobs.get(job_id, None).keys():

			for instance_type in jobs[job_id].get(utilization_class, None).keys():
				pool_used[utilization_class][instance_type] = (
						pool_used[utilization_class].get(instance_type, 0) -
						jobs[job_id][utilization_class][instance_type])
				if pool_used[utilization_class][instance_type] is 0:
					del pool_used[utilization_class][instance_type]
		del jobs[job_id]

	def rearrange_instances(self, jobs, pool_used, job):
		""" Moves job instance usage around when possible.

		If a job has relinquished its reserved instances, another job can
		pick them up. This function will remove all the currently pooled job
		instances and then re-pool to gain any reserved instances if there are some.

		Args:
			jobs: a dict of jobs currently running and the instances they use.

			pool_used: A dict of what instances are currently in use.

			job: Current job needing reallocation.

		Mutates:
			pool_used: May rearrange the instance usage if there is a better combination
			jobs: rearranges job instances.
		"""
		job_id = job.get('jobflowid')

		# Remove the jobs currently used instances then allocate the job over again.
		for utilization_class in jobs.get(job_id, None).keys():

			for instance_type in jobs[job_id].get(utilization_class, None).keys():
				pool_used[utilization_class][instance_type] = (
						pool_used[utilization_class].get(instance_type, 0) -
						jobs[job_id][utilization_class][instance_type]
					)
				if pool_used[utilization_class][instance_type] is 0:
					del pool_used[utilization_class][instance_type]

		self.allocate_job(jobs, pool_used, job)

	def _calculate_space_left(self, amt_used, utilization_class, instance_type):
		"""This function determines what utilization type we
		are doing in a kind of generic way. If the type is not
		a reserved instance, then the space left is infinite since
		we don't have to reserve that type upfront.
		"""
		if EC2.is_reserve_type(utilization_class):
			return self.pool[utilization_class].get(instance_type, 0) - amt_used
		else:
			return float('inf')

class SimulationObserver(object):
	"""Used to record information during each step of the simulation. 
	
	You can attach a SimulationObserver to a Simulator if you want information
	about each event. For example, the graph module uses the SimulationObserver
	to record the instances used during each event of the simulator so that it can
	graph used instances over time.
	"""
	def __init__(self, hour_graph, recorder):
		self.hour_graph = hour_graph
		self.recorder = recorder
	
	def update(self, time, node_type, job, data):
		"""Records data usage for each time node in the priority queue. The logger
		is called twice in a single event. So this records the state of the 
		simulator before and after some event

		Args:
			time: The datetime that the event occurred at.

			node_type: The event type (START, LOG or END)

			job: The individual job that is affected during this event.

			data: Currently either pool_used or logged_hours. You attach the
				observer to one of those (look at Simulator attach functions)
				and that is what data will become.
		"""

		for instance in job.get('instancegroups'):
			instance_type = instance.get('instancetype')

			# Add the time this event occurred at.
			current_time_line = self.hour_graph.get(instance_type, [])
			current_time_line.append(time)
			self.hour_graph[instance_type] = current_time_line

			# Grab the total usage logs for each util type.
			# It is a cumulative total to make a stacked graph which looks cooler.
			total = 0
			for utilization_class in EC2.ALL_UTILIZATION_PRIORITIES:
				if instance_type not in self.recorder[utilization_class]:
					self.recorder[utilization_class][instance_type] = []
				total += data[utilization_class].get(instance_type, 0)
				self.recorder[utilization_class][instance_type].append(total)


