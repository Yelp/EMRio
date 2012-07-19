EMRio
=====

Elastic MapReduce instance optimizer

EMRio helps you save money on Elastic MapReduce by using your last two 
months of usage to estimate how many EC2 reserved instances you should buy 
for the next year.

Introduction
------------
[Elastic MapReduce](http://aws.amazon.com/elasticmapreduce/) is a service provided by Amazon that makes it easy to use
MapReduce. EMR run on machines called EC2 instances. They come in many
different flavors from heavy memory usage to heavy CPU usage. When businesses
start using EMR, they use these services as a pay-as-you-go service. After
some time, the amount of instances you use can become stable. If you utilize 
enough instances over time, it might make sense to switch from the pay-as-you
-go service, or On-Demand service, to a pay-upfront service, or Reserved 
Instances service. 

How Reserved Instances work can be read
[here](http://aws.amazon.com/ec2/reserved-instances/). If you think that 
switching to reserved instances is a good plan, but don't know how many to 
buy, that's what EMRio is for!

How It Works
------------
EMRio first looks at your EMR history. That data has a two month limit. It 
then acts as if the job flow was reoccurring for a year. It has to estimate 
a year's worth of data for Reserved Instances to be worth the cost. It then 
simulates different configurations using the job flow history and will 
produce the best pool of instances to buy. 

Dependencies
------------
 * boto
 * tzinfo
 * matplotlib

Installation and Setup
----------------------
First, download the source

Then, go to the root directory and run:

`python setup.py`

Once you have the dependencies installed, you need to set up your boto 
configuration file. Look at our boto config as an example. Once you fill in 
the AWS key information and region information, copy it to either /etc/boto.
conf or ~/.boto

Running
-------
After you have the setup done, 

	emrio

This should take a minute or two to grab the information off S3, do a few 
simulations, and output the resultant optimized instance pool. 

If you want to see instance usage over time (how many instances are running 
at the same time), you run::

	emrio -g

After it calculates the same data, you will now see graphs of each instance-
type's usage over time, like this::

![picture Graph](http://github.com/Iph/EMRio/raw/master/docs/images/graph.png)

Now, re-calculating the optimal instances is kind of pointless on the same 
data, so in order to save and load optimal instance configurations, use this:

	emrio --cache=output.json

The format is json encoded, check out the tests folder where 
an example instance file can be found.

Which will save the results in output.txt, and load them like so:

	emrio --optimized=output.json

If you want to see all the commands, try `--help`.

	emrio --help


