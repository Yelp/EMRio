"""The optimizer module holds all the functions relating to creating the
best instance pool that yields the least cost over an interval of time.
"""
import copy
import logging
from math import ceil

from ec2_cost import instance_types_in_pool
from ec2_cost import fill_instance_types
from simulate_jobs import Simulator


class Optimizer(object):
    def __init__(self, job_flows, EC2, job_flows_interval=None):
        self.EC2 = EC2
        self.job_flows = job_flows
        self.job_flows_interval = job_flows_interval
        if job_flows_interval is None:
            min_time = min(job.get('startdatetime') for job in job_flows)
            max_time = max(job.get('enddatetime') for job in job_flows)
            self.job_flows_interval = max_time - min_time

    def run(self, pre_existing_pool=None):
        """Take all the max_instance counts, then use that to hill climb to
        find the most cost efficient instance cost

        Returns:
            optimal_pool: dict of the best pool of instances to be used.
        """
        if pre_existing_pool is None:
            optimized_pool = self.EC2.init_empty_reserve_pool()
        else:
            optimized_pool = pre_existing_pool

        # Zero-ing the instances just makes it so the optimized pool
        # knows all the instance_types the job flows use beforehand.
        fill_instance_types(self.job_flows, optimized_pool)
        for instance in instance_types_in_pool(optimized_pool):
            logging.debug("Finding optimal instances for %s", instance)
            self.optimize_reserve_pool(instance, optimized_pool)
        return optimized_pool

    def optimize_reserve_pool(self, instance_type, pool):
        """The brute force approach will take a single instance type and
        optimize the instance pool for it. By using the job_flows in
        simulations.

        Mutates: pool
        """
        simulator = Simulator(self.job_flows, pool, self.EC2)
        previous_cost = float('inf')
        current_min_cost = float("inf")
        current_cost = float('inf')
        current_min_instances = self.EC2.init_reserve_counts(pool,
            instance_type)

        # Calculate the default cost first.
        logged_hours = simulator.run()
        convert_to_yearly_estimated_hours(logged_hours,
            self.job_flows_interval)
        current_min_cost, _ = self.EC2.calculate_cost(logged_hours, pool)
        logging.debug('Current min cost: %s' % str(current_min_cost))
        current_cost = current_min_cost
        delta_reserved_hours = (
            self.delta_reserved_instance_hours_generator(instance_type, pool))

        while previous_cost >= current_cost:
            current_simulation_costs = (
                self.EC2.init_reserve_costs(float('inf')))
            # Add a single instance to each utilization type, and
            # record the costs. Choose the minimum cost utilization type.
            logging.debug("Simulation hours added %d",
                delta_reserved_hours.next())
            for utilization_class in pool:
                # Reset the min instances to the best values.
                for current_util in pool:
                    pool[current_util][instance_type] = (
                        current_min_instances[current_util])

                pool[utilization_class][instance_type] = (
                        current_min_instances[utilization_class] + 1)
                logged_hours = simulator.run()

                convert_to_yearly_estimated_hours(logged_hours,
                    self.job_flows_interval)
                cost, _ = self.EC2.calculate_cost(logged_hours, pool)
                current_simulation_costs[utilization_class] = cost
            previous_cost = current_cost
            current_cost = min(current_simulation_costs.values())
            min_util_level = None
            for utilization_class in current_simulation_costs:
                if current_simulation_costs[utilization_class] == current_cost:
                    min_util_level = utilization_class

            # Record the new cost, and see if adding one instance is better
            # If not, then break from the loop, since adding more will be worst
            if min(current_cost, current_min_cost) != current_min_cost or (
                current_cost == current_min_cost):

                current_min_cost = current_cost
                current_min_instances[min_util_level] += 1
            # Reset to best instance pool.
            for current_util in pool:
                pool[current_util][instance_type] = (
                    current_min_instances[utilization_class])
            logging.debug("Current best minimum cost for %s: %d",
                instance_type,
                current_min_cost)
        for utilization_class in current_min_instances:
            pool[utilization_class][instance_type] = (
                    current_min_instances[utilization_class])

    def delta_reserved_instance_hours_generator(self, instance_type, pool):

        starter_pool = copy.deepcopy(pool)
        assert(len(self.EC2.RESERVE_PRIORITIES) > 0)
        highest_util = self.EC2.RESERVE_PRIORITIES[0]
        iterative_simulator = Simulator(self.job_flows, starter_pool, self.EC2)
        previous_logged_hours = iterative_simulator.run()
        previous_hours = previous_logged_hours[highest_util][instance_type]

        while True:
            starter_pool[highest_util][instance_type] += 1
            current_logged_hours = iterative_simulator.run()
            current_hours = current_logged_hours[highest_util][instance_type]
            yield (current_hours - previous_hours)
            previous_hours = current_hours


def convert_to_yearly_estimated_hours(logged_hours, interval):
    """Takes a min and max time and will convert to the amount of hours
    estimated for a year.

    example: If interval was 2 months, we want the yearly cost
    so this would convert the 2 months into 60 days and then
    would multiply all the hours in logs by 365.0 / 60 to get the yearly
    amount used.

    Args:
        logs: The hours that each utilization type and each instance of that
            util that has been calculated in a simulation.

        interval: The span of time (timedelta) that the all the job flows
            ran in.

    Mutates:
        logs: Will multiply all the hours used by each instance type by the
            conversion rate calculated.

    Returns: nothing
    """
    days_per_year = 365.0
    conversion_rate = (days_per_year /
        (interval.days + interval.seconds / (24.0 * 60 * 60)))
    for utilization_class in logged_hours:
        for machine in logged_hours[utilization_class]:
            logged_hours[utilization_class][machine] = (
                ceil(logged_hours[utilization_class][machine] *
                    conversion_rate))
