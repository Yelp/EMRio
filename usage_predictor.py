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
	""" This is the same as the above function but for a single id"""

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
	"""Given a csv file, parse the file into an array of dicts. Each dict
	stores the key name given from the first line of the csv file, and the
	value is of the value of a row of data for that given key.
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
	""" If there is more than one id in the billing data, this will
	find each id and make it a key of a dict. The value is a list of
	objects that have that id in it.
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
	"""account is a list of objects corresponding to an account. This
	function just filters the billing objects and then displays the data
	in a graph.
	"""
	types = filter_bills(account)
	graph_bills(types, id)


def filter_bills(account):
	"""Since we only want EMR data usage, we need to remove any irrelevant billing
	information. This function will remove anything that doesn't use EMR and
	uses a hack that stores all the instance_types in a single key called 'match'
	to aggregate the data into a overall hours instead of individual types.
	Keeping it this way in case we want to change back to individual instance
	types, which is just removing the string 'match' to the variable match
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
	"""Will look at the standard deviation and mean and filter out
	things that are not within two standard devaitions of the mean.
	Returns 4 lists of usage, dates, outlier usage, and outlier dates.
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
	"""For each type of bill (instance type), filter the outliers and then
	display the regression line, relevant data points, and outliers"""
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
