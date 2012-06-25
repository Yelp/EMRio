"""These are the configurations needed to make EMRio run. 

TIME: is used to format the date of the graphs for EMRio. Change it
	if you want to use a different timezone for displaying the graphs.

EC2: This is the main calculations used to calculate EC2. There is a list of
	different cost zones available on Amazon's website. There is also a
	test price list in the tests folder if you want to update or create a
	new price list that is not currently in the EC2 folder.
"""
 
from ec2_cost import EC2Info
from ec2.west_coast_prices import COST, RESERVE_PRIORITIES
import pytz

EC2 = EC2Info(COST, RESERVE_PRIORITIES)
TIMEZONE = pytz.timezone('US/Alaska')
