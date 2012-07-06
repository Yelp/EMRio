"""These are the configurations needed to make EMRio run. 

TIME: is used to format the date of the graphs for EMRio. Change it
	if you want to use a different timezone for displaying the graphs.

EC2: This is the main calculations used to calculate EC2. There is a list of
	different cost zones available on Amazon's website.

	WARNING: If you are using this for calculations, the rates are pulled from
	Amazon's website here: http://aws.amazon.com/ec2/reserved-instances/
	The prices are calibrated for US West (Northern California).
	if you want to change that, you need to create your own file in
	ec2 folder and use it in config.py.

If you need a reference, take a look at test/test_price.py

"""
 
from ec2_cost import EC2Info
from ec2.west_coast_prices import COST, RESERVE_PRIORITIES
import pytz

EC2 = EC2Info(COST, RESERVE_PRIORITIES)
TIMEZONE = pytz.timezone('US/Alaska')
