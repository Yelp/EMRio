"""Job handler will pull and filter appropriate jobs.

Job handler will translate boto.job_flow objects to dicts,
translate the unicode dates to datetime objects, remove any job flows
that do not have start or end times and filter the min and max dates
input from the user.
"""

import datetime
import json
import logging

import boto.exception
from boto.emr.connection import EmrConnection


def get_job_flows(options, timezone):
    """Get job flows data from amazon's cluster or read job flows from
    a file.

    Args:
        options: An OptionParser object that has args stored in it.

    Returns:
        job_flows: A list of dicts of jobs that have run over a period of time.
    """
    job_flows = []
    if(options.file_inputs):
        job_flows = load_job_flows_from_file(options.file_inputs)
    else:
        job_flows = load_job_flows_from_amazon(options.conf_path,
            options.max_days_ago)

    job_flows = no_date_filter(job_flows)
    job_flows = convert_dates(job_flows, timezone)
    job_flows = range_date_filter(job_flows,
                                options.min_days,
                                options.max_days,
                                timezone)

    # sort job flows before running simulations.
    by_startdatetime = lambda j: j.get('startdatetime')
    job_flows = sorted(job_flows, key=by_startdatetime)
    return job_flows


def convert_dates(job_flows, timezone):
    """Converts the dates of all the jobs to the datetime object
    since they are originally in unicode strings

    Args:
        job: Current job being filtered.

    Mutates:
        job.startdatetime: Changes from unicode to datetime.
        job.enddatetime: Changes from unicode to datetime
    """

    for job in job_flows:
        job['startdatetime'] = parse_date(job['startdatetime'],
                                        timezone=timezone)
        job['enddatetime'] = parse_date(job['enddatetime'], timezone=timezone)

    return job_flows


def no_date_filter(job_flows):
    """Looks at the jobs and sees if they are missing a start or end date,
    which screws up simulations, so we remove them with this filter.

    Returns:
        Filtered job flows that only have full range of dates.
    """

    filtered_job_flows = []
    for job in job_flows:
        if job.get('startdatetime') and job.get('enddatetime'):
            filtered_job_flows.append(job)

    return filtered_job_flows


def range_date_filter(job_flows, min_days, max_days, timezone):
    """Removes any job that is not within the interval of min day and
    max day and returns the new filtered list.

    Returns:
        Returns job flows that ran within the interval of dates allowed.
    """

    filtered_job_flows = []
    if min_days:
        min_days = datetime.datetime.strptime(min_days, "%Y/%m/%d")
        min_days = min_days.replace(tzinfo=timezone)
    if max_days:
        max_days = datetime.datetime.strptime(max_days, "%Y/%m/%d")
        max_days = max_days.replace(tzinfo=timezone)
    for job  in job_flows:
        job_within_range = True
        if min_days and job['startdatetime'] < min_days:
            job_within_range = False
        if max_days and job['enddatetime'] > max_days:
            job_within_range = False

        if job_within_range:
            filtered_job_flows.append(job)
    return filtered_job_flows


def parse_date(str_date, timezone=None):
    """Changes a string that conforms to iso8601 to a non-naive datetime
    object.

    Args:
        str_date: string in the iso8601 format.

    Returns: datetime.datetime object in UTC tz.
    """
    current_date = datetime.datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%SZ")
    current_date = current_date.replace(tzinfo=timezone)
    return current_date


def load_job_flows_from_file(filename):
    """Loads job flows from a file specified by the filename. Will
    try comma-separated JSON objects then per-line objects before failing.
    """
    try:
        current_file = open(filename, 'r')
        contents = current_file.read().rstrip('\n')[:-1]
        job_flows = json.loads(contents)
        current_file.close()
        return job_flows
    except ValueError:
        print "Failed parsing pure json, trying back up format now..."
    job_flows = []
    current_file = open(filename, 'r')
    for line in current_file.readlines():
        job_flows.append(json.loads(line))
    current_file.close()
    return job_flows


def load_job_flows_from_amazon(conf_path, max_days_ago):
    """Gets all the job flows from amazon and converts them into
    a dict for compatability with loading from a file
    """
    now = datetime.datetime.utcnow()
    job_flows = get_job_flow_objects(conf_path, max_days_ago, now=now)
    dict_job_flows = []
    for job in job_flows:
        job_dict = job.__dict__
        new_list = []
        for instance in job.instancegroups:
            new_list.append(instance.__dict__)
        job_dict['instancegroups'] = new_list
        dict_job_flows.append(job_dict)
    job_flows = dict_job_flows
    return job_flows


def get_job_flow_objects(conf_path, max_days_ago=None, now=None):
    """Get relevant job flow information from EMR.

    Args:
        conf_path: is a string that is either None or has an alternate
            path to load the configuration file.

        max_days_ago: A float where if set, dont fetch job flows created
            longer than this many days ago.

        now: the current UTC time as a datetime.datetime object.
            defaults to the current time.
    Returns:
        job_flows: A list of boto job flow objects.
    """
    if now is None:
        now = datetime.datetime.utcnow()
    emr_conn = None
    emr_conn = EmrConnection()
    # if --max-days-ago is set, only look at recent jobs
    created_after = None
    if max_days_ago is not None:
        created_after = now - datetime.timedelta(days=max_days_ago)

    return describe_all_job_flows(emr_conn, created_after=created_after)


def describe_all_job_flows(emr_conn, states=None, jobflow_ids=None,
                            created_after=None, created_before=None):
    """Iteratively call ``EmrConnection.describe_job_flows()`` until we really
    get all the available job flow information. Currently, 2 months of data
    is available through the EMR API.

    This is a way of getting around the limits of the API, both on number
    of job flows returned, and how far back in time we can go.

    Args:
        states: A list of strings with job flow states wanted.

        jobflow_ids: A list of job flow IDs for jobs you want.

        created_after: a datetime object to limit job flows that are
            created after this date.

        created_before: same as created_after except before
    Returns:
        job_flows: A list of job flow boto objects
    """
    all_job_flows = []
    ids_seen = set()

    if not (states or jobflow_ids or created_after or created_before):
        created_before = (
            datetime.datetime.utcnow() + datetime.timedelta(days=1))

    while True:
        if created_before and created_after and created_before < created_after:
            break
        logging.disable(logging.DEBUG)
        logging.disable(logging.ERROR)
        logging.disable(logging.INFO)
        try:
            results = emr_conn.describe_jobflows(
                states=states, jobflow_ids=jobflow_ids,
                created_after=created_after, created_before=created_before)
        except boto.exception.BotoServerError, ex:
            if 'ValidationError' in ex.body:
                logging.disable(logging.NOTSET)
                break
            else:
                raise

        # don't count the same job flow twice
        job_flows = [jf for jf in results if jf.jobflowid not in ids_seen]
        all_job_flows.extend(job_flows)
        ids_seen.update(jf.jobflowid for jf in job_flows)

        if job_flows:
            # set created_before to be just after the start time of
            # the first job returned, to deal with job flows started
            # in the same second
            min_create_time = min(parse_date(jf.creationdatetime)
                                    for jf in job_flows)
            created_before = min_create_time + datetime.timedelta(seconds=1)
            # if someone managed to start 501 job flows in the same second,
            # they are still screwed (the EMR API only returns up to 500),
            # but this seems unlikely. :)
        else:
            if not created_before:
                created_before = datetime.utcnow()
            created_before -= datetime.timedelta(weeks=2)
    return all_job_flows
