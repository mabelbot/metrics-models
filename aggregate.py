import yaml
import elasticsearch
from elasticsearch import Elasticsearch, RequestsHttpConnection
from dateutil import parser
from datetime import datetime
import itertools
import collections
import logging

print(elasticsearch.__version__)

es = Elasticsearch("http://localhost:9200", use_ssl=False, verify_certs=False, ssl_no_validate=True,
                   connection_class=RequestsHttpConnection)

logging.basicConfig(filename='conversion_rate.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [AGGREGATE.PY]', level=logging.INFO)
logging.info('Begin aggregate.py')

# TODO
# Deal with issues -> pull requests later. Focus on D1 and D0 first of all as those are the easiest.
# Put logging into this module
# Put other config file into this module
# Config file for d0 d1, etc
# Check imports are compatible with versions (requirementx)
# Turn in memory to in-database for performance? Turn into batch processing?
# Turn this into a class
# Introduce conf
# Write tests
# Optimize runtime for larger datasets
# Allow D cutoffs to be not just numerical but include types of activities as well
# TODO specify docstrings

CONF = yaml.safe_load(open('conf.yaml'))
params = CONF['conversion-params']
print(CONF['conversion-params'])
D1_CUTOFF = params['d1-cutoff']
D2_CUTOFF = params['d2-cutoff']

# Query filter by author
query = {
    "bool": {
        "must": [
            #         { "match": { "actor_id": "44bdeb72febce7cd43244acdee5dab8dd6a702d8" }}
        ],
        "filter": [
            {"range": {
                "grimoire_creation_date": {
                    "gte": "1970-01-01",
                    "lt": datetime.now().strftime("%Y-%m-%d")
                }
            }
            }
        ]
    }
}

# Query Aggregate
# aggs = {

#         "contributions": {
#           "terms": { "field": "actor_id" }

#   }
# }


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

time_buckets = \
    es.search(index="github_event_enriched_combined", size=2, query=query, sort=sort, aggs=aggs)['aggregations'][
        'contribs_over_time']['buckets']


def contributors_d1(bucket_data):
    """
    Given bucket_data from the correct interval over which to assess,
    First groups all contributions by UUID, then filters to return only those contributors above D0 Cutoff
    """
    bucket_data.sort(key=lambda user: user['key'])
    contributions_by_uuid = list(dict((key, sum([pair['doc_count'] for pair in group])) for key, group in
                                      itertools.groupby(bucket_data, lambda user: user['key'])).items())

    return dict(filter(lambda x: int(x[1]) > D1_CUTOFF, contributions_by_uuid))


def contributors_d2(bucket_data):
    """
    Given bucket_data from the correct interval over which to assess,
    First groups all contributions by UUID, then filters to return only those contributors above D0 Cutoff
    """
    bucket_data.sort(key=lambda user: user['key'])
    contributions_by_uuid = list(dict((key, sum([pair['doc_count'] for pair in group])) for key, group in
                                      itertools.groupby(bucket_data, lambda user: user['key'])).items())

    return dict(filter(lambda x: int(x[1]) > D2_CUTOFF, contributions_by_uuid))


def get_contributors():  # TODO allow options
    bucket_start_dates = []
    cumulative_bucket_authors = []
    numerators = {}
    denominators = {}

    # Compare bucket i to i-1
    # test: Earliest is 2017-01-20 for Augur, first bucket is 2017-01-01 start
    # Procedure is to iterate over buckets until we get to the STARTING point of the interval, then compare with
    # the ENDING point of the interval to see the conversion rate for that interval.
    for i, result in enumerate(time_buckets):
        # info: Convert to <class 'datetime.datetime'>
        bucket_start_date = parser.parse(result['key_as_string'])

        # info: Keep track of start dates, especially the one from this
        bucket_start_dates.append(bucket_start_date)

        # info: Append the current bucket to the list of cumulative buckets (will have repeats)
        cumulative_bucket_authors.extend(result['actor']['buckets'])

        # info: get list of cumulative contributions as pairs of author uuid/count if they are over the d0 cutoff
        d1 = contributors_d1(cumulative_bucket_authors)
        denominators[bucket_start_date] = d1

        if i > 0:
            d2 = contributors_d2(cumulative_bucket_authors)
            numerators[bucket_start_date] = d2
    return numerators, denominators


def calculate_cr_series(numerators, denominators):
    """
    Calculate a series of conversion rates at fixed intervals by comparing contributors
    at different levels after a certain time interval.

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
        converters = numerators[date].keys() & denominators[timestamps[i]].keys()

        # Append tuple of conversion rate UP to this date.
        cr_series.append((date, len(converters) / len(denominators[timestamps[i]])))

    return cr_series


d2, d1 = get_contributors()  # Returns numerator, denominator (convert to, convert from)
result = calculate_cr_series(d2, d1)
