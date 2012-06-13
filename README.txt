EC2-RESERVE-COST-ANALYZER:

This tool can be used to read previous job flow data from Amazon's EC2 cluster. Once the data is read in, it uses the data provided and says how many reserved instances and of what type you should get. It will tell you the savings that would be earned if that job flow data you gave were to be consistent throughout a year. 

To use the tool, you can either use your current config file if it already connects to Amazons cluster to pull that information. 

Otherwise, you can use saved job flow data like so:

python audit_test --file file_name.json 

This will give basic stats and provide a graph of the instance useage using matplotlib.

You can use this graph data to see if there are any irregularities in the job flows, then limit the job flow to get a better prediction. 

Let's say you run the tool and see a graph like this:

PICTURE 1!#!@$#%#$^$%^%$^$%%$#@%@#

You check and see there is a large span of time that the job flows were running, talk it over with your team and find out that everything before that date was erratic and shouldn't be used in running your calculations. To limit dates, you can use the command line options: min-day, and max-day

So for the above example:
python audit_test --file file_name.json --min-day 2012/05/12 

which will limit the minimum start date to the 12th of May.

Just a word of warning, it could take a minute or two to calculate the optimal instances to be used, so if you want to save and load those results for graphs instead of waiting every time to look at a change in graphs, you can use the save and load options:

python audit_test --file file_name.json --save