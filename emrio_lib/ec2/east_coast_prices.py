# East Coast(Virginia) 1 Year Prices as of 6/14/2012
# data taken from:
# http://aws.amazon.com/ec2/pricing

HEAVY_UTIL = "Heavy Utility"
MEDIUM_UTIL = "Medium Utility"
LIGHT_UTIL = "Light Utility"
DEMAND = "Demand"
RESERVE_PRIORITIES = [HEAVY_UTIL, MEDIUM_UTIL, LIGHT_UTIL]
COST = {
	LIGHT_UTIL: {
		"m1.small": {
			'upfront': 69,
			'hourly': .039
		},
		'm1.medium': {
			'upfront': 138,
			'hourly': .078
		},
		'm1.large': {
			'upfront': 276,
			'hourly': .156
		},
		'm1.xlarge': {
			'upfront': 552,
			'hourly': .312
		},
		'micro': {
			'upfront': 23,
			'hourly': .012
		},
		'm2.xlarge': {
			'upfront': 353,
			'hourly': .22
		},
		'm2.2xlarge': {
			'upfront': 706,
			'hourly': .44
		},
		'm2.4xlarge': {
			'upfront': 1412,
			'hourly': .88
		},
		'c1.medium': {
			'upfront': 178,
			'hourly': .10
		},
		'c1.xlarge': {
			'upfront': 712,
			'hourly': .4
		},
		'cc1.4xlarge': {
			'upfront': 1450,
			'hourly': .742
		},
		'cc2.8xlarge': {
			'upfront': 1762,
			'hourly': .904
		},
		'cg1.4xlarge': {
			'upfront': 2410,
			'hourly': 1.234
		}
	},

	MEDIUM_UTIL: {
		"m1.small": {
			'upfront': 160,
			'hourly': .024
		},
		'm1.medium': {
			'upfront': 320,
			'hourly': 0.048
		},
		'm1.large': {
			'upfront': 640,
			'hourly': 0.096
		},
		'm1.xlarge': {
			'upfront': 1280,
			'hourly': 0.192
		},
		'micro': {
			'upfront': 54,
			'hourly': .007
		},
		'm2.xlarge': {
			'upfront': 850,
			'hourly': .133
		},
		'm2.2xlarge': {
			'upfront': 1700,
			'hourly': .266
		},
		'm2.4xlarge': {
			'upfront': 3400,
			'hourly': .532
		},
		'c1.medium': {
			'upfront': 415,
			'hourly': .06
		},
		'c1.xlarge': {
			'upfront': 1660,
			'hourly': .24
		},
		'cc1.4xlarge': {
			'upfront': 3284,
			'hourly': .45
		},
		'cc2.8xlarge': {
			'upfront': 4146,
			'hourly': .54
		},
		'cg1.4xlarge': {
			'upfront': 5630,
			'hourly': .74
		}
	},
	HEAVY_UTIL: {
		"m1.small": {
			'upfront': (195 + .016 * 24 * 365.0),
			'hourly': 0
		},
		'm1.medium': {
			'upfront': 390 + .032 * 24 * 365.0,
			'hourly': 0
		},
		'm1.large': {
			'upfront': 780 + .064 * 24 * 365.0,
			'hourly': 0
		},
		'm1.xlarge': {
			'upfront': 1560 + .128 * 24 * 365.0,
			'hourly': 0
		},
		'micro': {
			'upfront': 62 + .005 * 24 * 365.0,
			'hourly': 0
		},
		'm2.xlarge': {
			'upfront': 1030 + .088 * 24 * 365.0,
			'hourly': 0
		},
		'm2.2xlarge': {
			'upfront': 2060 + .176 * 24 * 365.0,
			'hourly': 0
		},
		'm2.4xlarge': {
			'upfront': 4120 + .352 * 24 * 365.0,
			'hourly': 0
		},
		'c1.medium': {
			'upfront': 500 + .04 * 24 * 365.0,
			'hourly': 0
		},
		'c1.xlarge': {
			'upfront': 2000 + .16 * 24 * 365.0,
			'hourly': 0
		},
		'cc1.4xlarge': {
			'upfront': 4060 + .297 * 24 * 365.0,
			'hourly': 0
		},
		'cc2.8xlarge': {
			'upfront': 5000 + .361 * 24 * 365.0,
			'hourly': 0
		},
		'cg1.4xlarge': {
			'upfront': 6830 + .494 * 24 * 365.0,
			'hourly': 0
		}
	},
	DEMAND: {
		"m1.small": {
			'upfront': 0,
			'hourly': .08
		},
		'm1.medium': {
			'upfront': 0,
			'hourly': .16
		},
		'm1.large': {
			'upfront': 0,
			'hourly': .32
		},
		'm1.xlarge': {
			'upfront': 0,
			'hourly': .64
		},
		'micro': {
			'upfront': 0,
			'hourly': .020
		},
		'm2.xlarge': {
			'upfront': 0,
			'hourly': .45
		},
		'm2.2xlarge': {
			'upfront': 0,
			'hourly': .90
		},
		'm2.4xlarge': {
			'upfront': 0,
			'hourly': 1.800
		},
		'c1.medium': {
			'upfront': 0,
			'hourly': .165
		},
		'c1.xlarge': {
			'upfront': 0,
			'hourly': .660
		},
		'cc1.4xlarge': {
			'upfront': 0,
			'hourly': 1.3
		},
		'cc2.8xlarge': {
			'upfront': 0,
			'hourly': 2.4
		},
		'cg1.4xlarge': {
			'upfront': 0,
			'hourly': 2.1
		}
	}
}
