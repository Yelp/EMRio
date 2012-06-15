# Usage Predictor is a separate tool to look at past usage and predict
# the future usage. It does so by attempting to find a linear regression
# curve and then looking further along the curve to find the future hour
# use. It creates a graph of what it is doing so that way a person can
# make sure that the points that the graph is using is valid or not.
# By valid, I mean there is the chance that billing can be erratic and so
# use of this tool would be limited if the graph shows erratic points that
# it used for the linear regression.

from optparse import OptionParser
from datetime import datetime
import csv
import sys
import re

import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

from ec2_cost import EC2


def main(args):
	option_parser = make_option_parser()
	options, args = option_parser.parse_args(args)
	billing_info = parse_billing_csv(options.file_input)
	different_bills = separate_ids(billing_info)
	for account in different_bills:
		show_billing_graph(different_bills[account], account)
	plt.show()


def predict_future_billing_multiplier(file_name, current_date, future_time,
									id):
	""" This function is used to pull data from a CSV file and
	then use that usage data to predict future usage hours.

	The current_date is used to have a base time that we want to get
	the usage hours for. The future time is the time we are trying to
	predict at.

	The return result will be a multiplier that you multiply the amount of
	hours by for each instance type to get to that future date hour usage.
	"""

	billing_info = parse_billing_csv(file_name)
	different_bills = separate_ids(billing_info)
	multiplier = 0

	# Until we can accurately know which ID is which, take the mean of all the
	# multipliers that are produced as the "real" multiplier
	if id in different_bills:
		types = filter_bills(different_bills[id])
		if len(types.keys()) != 0:
			predict_future(0, 0, types)
			multiplier = predict_future((current_date + future_time),
										different_bills[id])
	return multiplier


def predict_future(current_date, future_date, types):
	"""Uses linear regression to find a multiplier for future dates.

	In order to properly buy reserved instances in the future, we need
	a way to look into the future hours used. If we can find how many
	hours were used compared to the amount we are already using, we just
	multiply all the hours obtained from the simulations by a 'multiplier'
	which is obtained from the function.

	Args:
		future_date: a datetime object based on how far in the future you want
			to predict the amount of hours used to.

		current_date: a datetime object of the current time that we are at.
			This can be set to in the past if you want to test this function.

		types: a dict of instance_types with billed hours for them. Currently
			there is only one type called 'match' so that all the instance types
			are aggregated instead of separate.

	Returns:
		multiplier: A float to multiply hours by to get future usage hours.

	"""

	if types.get("match", None) is None:
		return 0
	usage_list, usage_dates, _, _ = filter_outliers(types["match"])
	date_list = mdates.date2num(usage_dates)
	current_date = mdates.date2num(current_date)
	future_date = mdates.date2num(future_date)

	# Calculate the slope of usage.
	A = np.vstack([date_list, np.ones(len(date_list))]).T
	m, c = np.linalg.lstsq(A, usage_list)[0]

	future_hour_use = m * future_date + c
	current_hour_use = m * current_date + c
	multiplier = future_hour_use / current_hour_use

	return multiplier


def make_option_parser():
	"""If you want to run this tool separate from the audit tool, then
	these options will be used instead
	"""
	usage = '%prog [options]'
	description = 'Print a giant report on EMR usage.'
	option_parser = OptionParser(usage=usage, description=description)
	option_parser.add_option(
		'-f', '--file', dest='file_input', type='string', default=None,
		help="Input a file instead of sending request to server"
		)
	return option_parser


def parse_billing_csv(file_name):
	"""Parses a file into CSV objects.

	This function takes a csv filename as input and will use that to break
	down each line into an object where the column rows are the keys and the
	rows values are the values.

	Args:
		file_name: Where the CSV file is located.

	Returns:
		csv_objects: A list of column: row-value pairs parsed from the CSV files.

	"""
	parsed_csv = []
	with open(file_name, 'rb') as f:
		reader = csv.reader(f)
		rows = [row for row in reader]
		names = rows[0]
		for row in rows[1:]:
			csv_object = {}
			for name_col in range(len(names)):
				csv_object[names[name_col]] = row[name_col]
			parsed_csv.append(csv_object)
	return parsed_csv


def separate_ids(billing_info):
	""" Separates CSV objects into individual account types.

	An Amazon Billing CSV file can have more than one account type
	for billing purposes. We usually want to isolate one of those
	account types, so this function puts CSV objects into individual
	account ids, so that we know which bill goes with which account.

	Args:
		billing_info: A list of CSV objects where the keys are CSV columns
			and the rows are the values.

	Returns:
		accounts: A dict with keys that are account types and values that are
			lists of CSV objects that have that account id.
	"""
	# Identify all the ids in the info
	ids = set()
	separated_bills = {}

	for bill in billing_info:
		if bill["Account Id"] not in ids:
			ids.add(bill["Account Id"])

	for acc_id in ids:
		separated_bills[acc_id] = []
		for bill in billing_info:
			if bill["Account Id"] == acc_id:
				separated_bills[acc_id].append(bill)

	return separated_bills


def show_billing_graph(account, id):
	"""Creates a graph with the account info.

	This function will take an account's CSV object list and use it to make
	make a graph showing the billing amount for each month, then make a
	linear regression line on it to show what the slope for the multiplier
	used in predict_future is in graph form. It shows the outliers not used
	as well which is helpful.

	Args:
		account: A list of CSV objects for a specific account to graph.
		id: The account id to display on the name of the graph.

	Returns:
		nothing
	"""
	types = filter_bills(account)
	graph_bills(types, id)


def filter_bills(account):
	"""Filters out data that is not EMR billing info.

	The CSV objects hold things from S3 storage to Cloud Compute costs.
	We don't care about most of this data, except for EMR data. This function
	takes a CSV object list and will remove any objects from it, and then
	classify the EMR instance type billing that it belongs to.

	NOTE: As of right now, type aggregates all the CSV instance types into
	a cumulative sum called "match". To change this, remove the string and
	put match variable in.

	Args:
		account: List of CSV objects for a given account.

	Returns:
		types: A dict filled with aggregate sums of billing for a given
			instance type (currently total aggregate sum). It is structured
			where the bill_date is the key, and the usage amount is the value.
	"""
	types = {}
	for bill in account:
		# We only care about EMR billing. Cut the rest.
		if bill["Product Name"] != "Amazon Elastic MapReduce":
			continue
		matches = re.findall('\((.*?)\)', bill["Item Description"])
		for match in matches:
			if match in EC2.TYPE_NAMES:
				bill_list = types.get("match", {})
				# Format used by Amazon for billing.
				start_date = datetime.strptime(bill["Start Date"], "%Y-%m-%d %H:%M:%S UTC")
				bill_list[start_date] = (bill_list.get(start_date, 0) +
										float(bill["Usage Amount"]))
				types["match"] = bill_list
	return types


def filter_outliers(i_type_nodes):
	"""Remove billing months with huge deviations from other billing months.

	Computes the standard deviation and will remove and billing months that have
	large differences from the mean amount.

	Args:
		i_type_nodes: A dict where the keys are the datetimes that billing occured
			and values are the amount of hours used for that date.

	Returns (In order):
		usage_list: A list of all the nodes that are not outliers.

		date_list: A list of dates that do not have outliers. Index is
			associatively related to usage_list.

		outlier_list: A list of all nodes that ARE outliers.

		outlier_dates: A list of dates that the outliers occurred on. Index is
			associatively related to outlier_list.
	"""
	date_list = []
	usage_list = []
	outlier_list = []
	outlier_dates = []
	computation_list = []

	key_list = i_type_nodes.keys()
	key_list.sort()
	for date_node in key_list:
		computation_list.append(i_type_nodes[date_node])

	# Calculate the bounds of acceptable data values.
	standard_deviation = np.std(computation_list)
	mean = np.mean(computation_list)
	lower_bound = mean - 2 * standard_deviation
	upper_bound = mean + 2 * standard_deviation

	# Filter out the outliers into its own list of dates and
	# usage points.
	for date_node in key_list:
		usage_stat = i_type_nodes[date_node]
		if usage_stat >= lower_bound and usage_stat <= upper_bound:
			date_list.append(date_node)
			usage_list.append(i_type_nodes[date_node])
		else:
			outlier_list.append(usage_stat)
			outlier_dates.append(date_node)
	return usage_list, date_list, outlier_list, outlier_dates


def graph_bills(types, id):
	"""The function that graphs all the bills.

	Args:
		types: A dict where the key is the instance type and values
			are dicts of datetime_billed: hours_used.

		id: Name of the account where the billing info came from.

	Returns: Nothing.
	"""
	for i_type in types:
		months = mdates.MonthLocator()
		formatter = mdates.DateFormatter("%m/%Y ")

		usage_list, date_list, outlier_list, outlier_dates = (
			filter_outliers(types[i_type])
		)
		fig = plt.figure()
		fig.suptitle('EC2 Monthly Hour Usage for Account: ' + id)
		ax = fig.add_axes([0.1, 0.1, 0.6, 0.75])
		date_list = mdates.date2num(date_list)
		A = np.vstack([date_list, np.ones(len(date_list))]).T
		outlier_dates = mdates.date2num(outlier_dates)
		m, c = np.linalg.lstsq(A, usage_list)[0]

		ax.plot(date_list, usage_list, 'o', color="#B70000",
			label="Relevant Data Points")
		ax.plot(outlier_dates, outlier_list, 'o', color='#FFFFFF',
			label="Outliers (2 st dev)")
		ax.plot(date_list, (m * date_list + c), 'r', label="Regression line")

		ax.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

		ax.xaxis.set_major_locator(months)
		ax.xaxis.set_major_formatter(formatter)

		plt.subplots_adjust(bottom=0.15)
		ax.set_xlabel("Monthly bill dates")
		ax.set_ylabel("Hours Used Overall")
		fig.autofmt_xdate()
if __name__ == '__main__':
	main(sys.argv[1:])
