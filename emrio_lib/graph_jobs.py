""" Graphing tools for EMRio.

This tool uses an observer to pull information from a simulation of jobs.
Once it has that information, it will use hours recorded and matplotlib to make
graphs from the job flows.
"""
import copy

from ec2_cost import instance_types_in_pool
from simulate_jobs import Simulator, SimulationObserver


class Grapher(object):
    def __init__(self, job_flows, pool, EC2):
        """Grapher will set up graphs to be shown based
        on the job flow and pools given.

        Args:
            job_flows: A list of job flow dict objects that are to
                be graphed.

            pool: The pool of reserved instances to use for applying
                usage on the graphs.

            EC2: An EC2Info object to output costs and run simulations.

        """
        self.pool = pool
        self.job_flows = job_flows
        self.EC2 = EC2
        self.colors = self.EC2.color_scheme()

    def show(self, total_usage=False, instance_usage=False):
        """This will make and show the graphs for the grapher class.
        It will accumulate all the graphs you want to see.

        Args:
            total_usage: A boolean option to graph total hours used
                over a job flow history.

            instance_usage: A boolean option to graph instance usage
                over a job flow history.
        """
        self.plt = None
        if total_usage or instance_usage:
            try:
                import matplotlib.dates as mdates
                import matplotlib.pyplot as plt
            except ImportError:
                print "You need matplotlib installed to use graphs. Please"
                " install and try again."
            self.plt = plt
            self.mdates = mdates
        if total_usage:
            self._total_hours_graph()
            self.graph_over_time(self.total_hours_over_time,
                self.total_hours)
        elif instance_usage:
            self._instance_usage_graph()
            self.graph_over_time(self.used_instances_over_time,
                self.instance_hours)
        if self.plt:
            self.plt.show()

    def _total_hours_graph(self):
        """Graph the total hours used by reserved / on demand
        instances over a period of time"""
        logged_hours_over_time, hours = self.record_log_data()
        self.total_hours_over_time = logged_hours_over_time
        self.total_hours = hours

    def _instance_usage_graph(self):
        """This will graph the instances used and the type of
        instance used over time.
        """
        used_instances, hours = self.record_used_instances()
        self.used_instances_over_time = used_instances
        self.instance_hours = hours

    def record_used_instances(self):
        """Stores information regarding what instances were in the
        'used_pool' during the job simulation at all points of the
        simulation.
        """
        used_instances_over_time = self.EC2.init_empty_all_instance_types()
        event_times = {}
        instance_simulator = Simulator(self.job_flows, self.pool, self.EC2)
        observer = SimulationObserver(event_times, used_instances_over_time)
        instance_simulator.attach_pool_use_observer(observer)
        instance_simulator.run()
        return used_instances_over_time, event_times

    def record_log_data(self):
        """This will set up the record information to graph total hours
        logged in a simulation over time.
        """
        logged_hours_per_hour = self.EC2.init_empty_all_instance_types()
        event_times = {}
        log_simulator = Simulator(self.job_flows, self.pool, self.EC2)
        observer = SimulationObserver(event_times, logged_hours_per_hour)
        log_simulator.attach_log_hours_observer(observer)
        log_simulator.run()

        return  logged_hours_per_hour, event_times

    def graph_over_time(self, info_over_time,
                hours_line,
                xlabel='Time job ran (in hours)',
                ylabel='Instances run'):
        """Given some sort of data that changes over time, graph the
        data usage using this"""

        begin_time = min(job.get('startdatetime') for job in self.job_flows)
        end_time = max(job.get('enddatetime') for job in self.job_flows)

        # If end time is during the day, round to the next day so graph looks
        # pretty.
        if end_time.hour != 0:
            end_time = end_time.replace(hour=0, day=(end_time.day + 1))

        for instance_type in instance_types_in_pool(info_over_time):
            # Locators / Formatters to pretty up the graph.
            hours = self.mdates.HourLocator(byhour=None, interval=1)
            days = self.mdates.DayLocator(bymonthday=None, interval=1)
            formatter = self.mdates.DateFormatter("%m/%d ")

            fig = self.plt.figure()
            fig.suptitle(instance_type)
            ax = fig.add_subplot(111)
            date_list = self.mdates.date2num(hours_line[instance_type])

            all_utilization_classes = copy.deepcopy(
                            self.EC2.ALL_UTILIZATION_PRIORITIES)

            # Reverse so demand is graphed first, it should be the largest.
            all_utilization_classes.reverse()

            for utilization_class in all_utilization_classes:
                ax.plot(date_list,
                    info_over_time[utilization_class][instance_type],
                    color='#000000')
                ax.plot(date_list[0],
                    info_over_time[utilization_class][instance_type][0],
                    color=self.colors[utilization_class],
                    label=utilization_class)
                ax.fill_between(date_list,
                    info_over_time[utilization_class][instance_type],
                    color=self.colors[utilization_class],
                    alpha=1.0)

            ax.xaxis.set_major_locator(days)
            ax.xaxis.set_major_formatter(formatter)
            ax.xaxis.set_minor_locator(hours)

            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            ax.set_xlim(begin_time, end_time)
            ax.grid(True)
            ax.legend()
            self.plt.xticks(rotation='vertical')
