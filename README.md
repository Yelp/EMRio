EMRio
=====

Elastic MapReduce instance optimizer

Introduction
------------
Elastic MapReduce is a service provided by Amazon that makes it pretty easy to use MapReduce. EMR jobs run on machines called EC2 instances. They come in many different flavors from heavy memory usage to heavy CPU usage jobs. When businesses start using EMR, they use these services as a pay-as-you-go service, which is nice. After some time though, the amount of instances you use can become fairly stable though. If you utilize enough instances over time, it might make sense to switch from the pay-as-you-go service, or On-Demand service, to a pay-upfront service, or Reserved Instances service. 

Reserved Instances work by paying an upfront price and then paying less hourly cost than the On-Demand service. You buy a Reserved Instance for a year or three years, then you have to renew it after that term is up. They also come in three different types: Heavy, Medium and Light utilization. These utilization classes offer more diverse options based on how much you use the EC2 machines. The question is though, how many instances should you buy and of what utilization class? This is the question EMRio tries to answer! 

How It Works
------------
EMRio first looks at your previous job flow usage that you supply it. If you don't supply a file to EMRio, it uses Amazon data with either Boto or mrjobs. That data has a two month limit, so it is a good idea to save that history every two months and combine them to get better results later down the road. It then acts as if the job flow was reoccurring for a year -- in case you supply less than a years worth of data, it has to estimate a year's worth of data for Reserved Instances to be worth the cost. It then simulates different configurations using the job flow history and will produce the best pool of instances to buy. 

Dependencies
------------
	-mrjob(or boto)
	-tzinfo
	-matplotlib
How to Run EMRio
----------------
Once you have the dependencies installed, you just run::

	python EMRio.py

This should take a minute or two to grab the information off S3, do a few simulations, and output the resultant optimized instance pool. 

If you want to see instance usage over time (how many instances are running at the same time), you run::

	python EMRio.py --graph cost

After it calculates the same data, you will now see graphs of each instance-type's usage over time, like this::

	IMAGE HERE

Now, re-calculating the optimal instances is kind of pointless on the same data, so in order to save and load optimal instance configurations, use this:

	python EMRio.py --save-optimized=output.txt

If you want to see how this is formatted, check out the tests folder where an example instance file can be found.

Which will save the results in output.txt, and load them like so:

	python EMRio.py --optimized=output.txt

If you want to see all the commands, try --help.
