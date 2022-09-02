import yaml
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from dateutil import parser
from datetime import datetime, timedelta # TODO https://stackoverflow.com/questions/12906402/type-object-datetime-datetime-has-no-attribute-datetime
from dateutil.relativedelta import *
import itertools
import collections
from collections import deque
import logging

MAX_BULK_UPDATE_SIZE = 100

print(elasticsearch.__version__)

es = Elasticsearch("http://localhost:9200", use_ssl=False, verify_certs=False, ssl_no_validate=True,
                   connection_class=RequestsHttpConnection)

logging.basicConfig(filename='conversion_rate.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [AGGREGATE.PY]', level=logging.INFO)
logging.info('Begin aggregate.py')

# TODO
# Introduce secondary filtering for D levels
# Write back into index
# Deal with issues -> pull requests later.
# Config file for d0 d1, etc
# Check imports are compatible with versions (requirementx)
# Turn in memory to in-database for performance? Turn into batch processing?
# Turn this into a class
# Write tests
# Optimize runtime for larger datasets
# Allow D cutoffs to be not just numerical but include types of activities as well
# Put results back into elasticsearch for visualization later
# specify docstrings fully, refactor d cutoff methods
# Follow sphinx documentation style
# Manage default number of shards
# Introduce support for day lag
# Turn into a class
# Make it more flexible for interval (monthly is the only one allowed now)

# Import Config File
CONF = yaml.safe_load(open('conf.yaml'))
params = CONF['conversion-params']

# Set Parameters from Imported Config File
D1_CUTOFF = params['denominator-cutoff']
D2_CUTOFF = params['numerator-cutoff']
OUT_INDEX_NAME = params['final-out-index']

# Number of months to look back for contributor interval
# Some info: Look back interval is actually min(months since start, TRACKING_LAG_PERIOD)
# Also, if TRACKING_LAG_PERIOD is 6, and the bucket is 2020-08-01 for example,
# It actually means that there will be data considered from August 2020, and all 5 months prior to August
# (March, April, May, June, July).
TRACKING_LAG_PERIOD = CONF['tracking-params']['tracking_interval_num_months']
CONTRIBUTION_TYPES_DENOMINATOR = CONF['conversion-params']['denominator_events']
CONTRIBUTION_TYPES_NUMERATOR = CONF['conversion-params']['numerator_events']
START_TIME = (datetime.strptime('1970-01-01', "%Y-%m-%d") + relativedelta(months=+6)).strftime("%Y-%m-%d")

print(f"Cutoffs {D1_CUTOFF}, {D2_CUTOFF} / tracking period {TRACKING_LAG_PERIOD} months")


# TODO add more options here

# Make sure index clean before use if exists (syntax will be different in ES v. 8+)
es.indices.delete(index=OUT_INDEX_NAME, ignore=[400, 404])


query_denominator = {
    "bool": {
        "must": [
            #         { "match": { "actor_id": "44bdeb72febce7cd43244acdee5dab8dd6a702d8" }}
        ],
        "filter": [
            {"range": {
                "grimoire_creation_date": {
                    "gte": '1970-01-01',
                    "lt": datetime.now().strftime("%Y-%m-%d")
                }
            }
            },
            {
                "terms": {
                    "event_type": CONTRIBUTION_TYPES_DENOMINATOR
                }
            }
        ]
    }
}


query_numerator = {
    "bool": {
        "must": [
            #         { "match": { "actor_id": "44bdeb72febce7cd43244acdee5dab8dd6a702d8" }}
        ],
        "filter": [
            {"range": {
                "grimoire_creation_date": {
                    "gte": '1970-01-01',
                    "lt": datetime.now().strftime("%Y-%m-%d")
                }
            }
            },
            {
                "terms": {
                    "event_type": CONTRIBUTION_TYPES_NUMERATOR
                }
            }
        ]
    }
}


# Aggregate per month, then aggregate per actor_id
aggs = {
    "contribs_over_time": {
        "date_histogram": {
            "field": "grimoire_creation_date",
            "interval": "month"
        },
        "aggs": {  # 2nd level sub-bucket aggregation
            "actor": {
                "terms": {
                    "field": "actor_id"
                }
            }
        }
    }
}

sort = {
    "grimoire_creation_date": {"order": "asc"}
}

# Note that time buckets will be grouped by a date and a time of 00:00, such as '2016-12-31T00:00:00.000Z'
# This bucket is a START interval, so it counts everything from that date until the day BEFORE the next
# bucket's start date, at 11:59pm
# The types of contributions filtered with the numerator and denominator are different, so they will need different queries
# TODO adjust this to work with less in-memory computation
time_buckets_denominator = es.search(index="github_event_enriched_combined",
                         size=2,
                         query=query_denominator,
                         sort=sort,
                         aggs=aggs)['aggregations']['contribs_over_time']['buckets']

time_buckets_numerator= es.search(index="github_event_enriched_combined",
                         size=2,
                         query=query_numerator,
                         sort=sort,
                         aggs=aggs)['aggregations']['contribs_over_time']['buckets']

print("breakpoint")

def contributors_filtered_by_cutoff(bucket_data, lower_cutoff, upper_cutoff):
    """
    Given bucket_data from the correct interval over which to assess,
    First groups all contributions by UUID, then filters to return only those contributors between
    the lower and upper cutoff (inclusive)


    :param bucket_data: list of dict[str, int] objects of the form {'key': str key, 'doc_count': x} corresponding to one
        or more combined elasticsearch time interval buckets
    :param cutoff: integer cutoff to filter contributions
    :returns: dict[str, int] where the keys are uuid and the values are number of contributions
        dict can be empty if the input is empty
    :raises IndexError: if bucket_data is ill formed or empty
    """
    if not lower_cutoff:
        lower_cutoff = 0
    if not upper_cutoff:
        upper_cutoff = float("inf")
    bucket_data.sort(key=lambda user: user['key'])
    contributions_by_uuid = list(dict((key, sum([pair['doc_count'] for pair in group])) for key, group in
                                      itertools.groupby(bucket_data, lambda user: user['key'])).items())

    return dict(filter(lambda x: lower_cutoff <= int(x[1]) <= upper_cutoff, contributions_by_uuid))


def get_contributors(time_buckets_denominator, time_buckets_numerator):
    """
    Prepares 2 dictionaries for further computation - one for numerator and one for denominator.
    Length of numerators dictionary will be of length 1 shorter than denominator.

    Handles edge case for people jumping straight to higher level on first month
    by adding them retroactively to the denominator of the month before to be included
    in a conversion for this month. 
    These people are not "true" conversions but must be included for completeness. TODO

    Buckets begin at beginning of month with the first contributions,
    e.g. Earliest contribution is 2017-01-20 for Augur, first bucket is 2017-01-01 start

    :param time_buckets_denominator: data from ES query
    :param time_buckets_numerator: data from ES query
    :returns: dict[str, dict[str, int]] where they keys are timestamps and the values are 
        dictionaries of uuid: count of valid contributions 
    """
    numerators = {}
    denominators = {}
    converters = set()
    date_of_first_contribution_by_uuid = {}
    buckets_in_consideration_numerator = deque()  # Contains only data within a Lag Time behind.
    buckets_in_consideration_denominator = deque()  # Contains only data within a Lag Time behind.
    last_date_out_of_range_denominator = []  # This list will be empty until we have passed at least Lag Time months from start
    time_buckets = list(zip(time_buckets_denominator, time_buckets_numerator)) # Zip into tuple for shorter implementation
    
    for i, bucket in enumerate(time_buckets):
        # info: Use a current bucket (numerator or denominator does not matter) just to get the dates required
        bucket_start_date = parser.parse(bucket[0]['key_as_string'])
        prev_bucket_start_date = parser.parse(time_buckets[i-1][0]['key_as_string']) if i > 0 else None

        # info: Append the current bucket to the list of buckets in consideration
        if len(buckets_in_consideration_numerator) >= TRACKING_LAG_PERIOD:
            assert len(buckets_in_consideration_numerator) == len(buckets_in_consideration_denominator)
            buckets_in_consideration_numerator.popleft()
            last_date_out_of_range_denominator = buckets_in_consideration_denominator.popleft()
            # TODO test if the right item is popped
        else:
            buckets_in_consideration_denominator.append(bucket[0]['actor']['buckets'])
            buckets_in_consideration_numerator.append(bucket[1]['actor']['buckets'])
            # TODO check if in correct order

        # info: Flatten the current deque - cumulative_bucket_authors is a LIST of {'key': , 'doc_count: } dictionaries
        cumulative_bucket_authors_denominator = [val for sublist in list(buckets_in_consideration_denominator) for val in sublist]
        cumulative_bucket_authors_numerator = [val for sublist in list(buckets_in_consideration_numerator) for val in sublist]

        # info: Collect first contribution date from the numerator for edge case
        for j, c in enumerate(bucket[1]['actor']['buckets']):
            if c['key'] not in date_of_first_contribution_by_uuid:
                date_of_first_contribution_by_uuid[c['key']] = bucket_start_date

        # info: Perform filtering of denominator by numerical cutoff - d1 is a flat dictionary of uuid: count
        d1 = contributors_filtered_by_cutoff(cumulative_bucket_authors_denominator, D1_CUTOFF, D2_CUTOFF)  # TODO need this cutoff or else will double count in denominator
        denominators[bucket_start_date] = d1

        if i > 0:
            d2 = contributors_filtered_by_cutoff(last_date_out_of_range_denominator + cumulative_bucket_authors_numerator, 
                                                    D2_CUTOFF + 1,
                                                    None)

            # info: Initially, we filtered to include first time contributors only to prevent re-conversions.
                # Now, it is allowed. TODO allow option to disallow?
            numerators[bucket_start_date] = d2

            first_time_D2 = dict(filter(lambda x: date_of_first_contribution_by_uuid[x[0]] == bucket_start_date,
                                                        d2.items()))
            denominators[prev_bucket_start_date].update(first_time_D2)

            converters.update(d2.keys())

    return numerators, denominators


def calculate_cr_series(numerators, denominators):
    """
    Calculate a series of conversion rates at fixed intervals by comparing contributors
    at different levels after a certain time interval.
    Reindex the result.

    # Compare bucket i to i-1
    # Procedure is to iterate over buckets until we get to the STARTING point of the interval, then compare with
    # the ENDING point of the interval to see the conversion rate for that interval.

    Numerators = level to
    Denominators = level from

    Numerators and denominators must be of length n and n+1 respectively,
    giving us n conversion rates.
    Time stamps must match with denominators consisting of 1 earlier time stamp and
    all others shared with numerators.

    Divides numerator at time t with denominator at time t-1 to get a conversion rate.
    """
    assert len(denominators) - len(numerators) == 1
    numerators = collections.OrderedDict(sorted(numerators.items()))  # Assure dates line up
    denominators = collections.OrderedDict(sorted(denominators.items()))  # Assure dates line up

    timestamps = list(denominators.keys())
    cr_series = []

    # Step through timestamps as numerator timestamps (for denominator, look 1 interval backwards)
    for i, date in enumerate(timestamps[1:]):
        logging.info(f"Now calculating CR for {date} with {numerators[date]} and {denominators[timestamps[i]]}")

        # Find uuids who completed conversion (shared between sets)
        # This excludes people who did not pass through the denominator phase by the time the denominator was
        # calculated, but somehow ended up in the numerator phase by the time it was calculated
        # This is due to the need to exclude people who have already been participating as conversions. TODO
        # One person makes > D1 cutoff contributions in their first month, then they're not counted in the denominator?
        # But if you dont use any cutoff, then you can get the same person converting twice. Which does not make sense.
        # The third solution is to just count anyone who meets the D2 cutoff over anyone who makes the D1 cutoff but
        # not the D2 one
        # So there is an issue of people who never pass through the "new" phase given a certain
        # cutoff and calculation interval
        # This line will handle no duplicate conversions after the denominator excludes non-new contributions first
        converters = numerators[date].keys() & denominators[timestamps[i]].keys()

        # Append tuple of conversion rate UP to this date.
        cr_series.append((date, len(converters) / len(denominators[timestamps[i]]))
                         if len(denominators[timestamps[i]]) else 0)
        doc = {
            "_index": OUT_INDEX_NAME,
            "_type": "_doc",
            "_source": {
                'date_of_conversion': date,
                'conversion_rate': len(converters) / len(denominators[timestamps[i]])
                    if len(denominators[timestamps[i]]) else float(0),
                'num_converters': int(len(converters)),
                'converters': list(converters)
            }
        }
        yield doc

    return cr_series

# Toggle these on for debug
d2, d1 = get_contributors(time_buckets_denominator, time_buckets_numerator)  # Returns numerator, denominator (convert to, convert from)
helpers.bulk(es, calculate_cr_series(d2, d1)) # Bulk upload
