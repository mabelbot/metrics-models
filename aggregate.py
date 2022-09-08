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

# TODO
# Deal with issues -> pull requests later.
# Check imports are compatible with versions (requirementx)
# Turn in memory to in-database for performance? Turn into batch processing?
# Write tests
# Optimize runtime for larger datasets
# Allow D cutoffs to be not just numerical but include types of activities as well
# Manage default number of shards
# Overlapping visualizations for multiple communities
# Introduce support for non month lag periods and test the other sizes of lag times
# Make it more flexible for interval (monthly is the only one allowed now)


class Aggregate():
    def __init__(self, *args, **kwargs):
        self.es = Elasticsearch("http://localhost:9200", use_ssl=False, verify_certs=False, ssl_no_validate=True, connection_class=RequestsHttpConnection)

        logging.basicConfig(filename='aggregate.log', filemode='w',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [AGGREGATE.PY]', level=logging.INFO)
        logging.info('Begin aggregate.py')


        # Set Parameters from Imported Config File
        self.d1_cutoff = kwargs.get('conversion-params').get('denominator-cutoff')
        self.d2_cutoff = kwargs.get('conversion-params').get('numerator-cutoff')
        self.repo_name = args[0]
        self.project_name = args[1]
        self.out_index_base = kwargs.get('general').get('out_index_base')
        self.index_to_query_suffix = args[2]
        self.out_index_name = kwargs.get('conversion-params').get('final-out-index') + '_' + self.repo_name + '_' + self.project_name
        self.from_date = kwargs.get('tracking-params').get('from_date')
        if not self.from_date:
            from_date = '1970-01-01'

        # Number of months to look back for contributor interval
        # Some info: Look back interval is actually min(months since start, self.tracking_lag_period)
        # Also, if self.tracking_lag_period is 6, and the bucket is 2020-08-01 for example,
        # It actually means that there will be data considered from August 2020, and all 5 months prior to August
        # (March, April, May, June, July).
        self.tracking_lag_period = kwargs.get('tracking-params').get('tracking_interval_num_months')
        self.contribution_types_denominator = kwargs.get('conversion-params').get('denominator_events')
        self.contribution_types_numerator = kwargs.get('conversion-params').get('numerator_events')
        self.start_time = (datetime.strptime('1970-01-01', "%Y-%m-%d") + relativedelta(months=+6)).strftime("%Y-%m-%d")
        self.allow_multiple_conversions = kwargs.get('conversion-params').get('allow_multiple_conversions')

        print(f"Cutoffs {self.d1_cutoff}, {self.d2_cutoff} / tracking period {self.tracking_lag_period} months")

        # Make sure index clean before use if exists (syntax will be different in ES v. 8+)
        self.es.indices.delete(index=self.out_index_name, ignore=[400, 404])


        # Prepare queries
        query_denominator = {
            "bool": {
                "must": [
                #         { "match": { "actor_id": "44bdeb72febce7cd43244acdee5dab8dd6a702d8" }}
                ],

                "filter": [
                    {"range": {
                        "grimoire_creation_date": {
                            "gte": self.from_date,
                            "lt": datetime.now().strftime("%Y-%m-%d")
                        }
                    }
                    },
                    {
                        "terms": {
                            "event_type": self.contribution_types_denominator
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
                {
                    "range": {
                    "grimoire_creation_date": {
                        "gte": self.from_date,
                        "lt": datetime.now().strftime("%Y-%m-%d")
                    }
                    }
                },
                {
                    "terms": {
                        "event_type": self.contribution_types_numerator
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
        self.time_buckets_denominator = self.es.search(index=self.out_index_base + "_" + self.index_to_query_suffix,
                        size=2,
                        query=query_denominator,
                        sort=sort,
                        aggs=aggs)['aggregations']['contribs_over_time']['buckets']

        self.time_buckets_numerator = self.es.search(index=self.out_index_base + "_" + self.index_to_query_suffix,
                        size=2,
                        query=query_numerator,
                        sort=sort,
                        aggs=aggs)['aggregations']['contribs_over_time']['buckets']


    def contributors_filtered_by_cutoff(self, bucket_data, lower_cutoff, upper_cutoff):
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


    def get_contributors(self):
        """
        Prepares 2 dictionaries for further computation - one for numerator and one for denominator.
        Length of numerators dictionary will be of length 1 shorter than denominator.

        Handles edge case for people jumping straight to higher level on first month
        by adding them retroactively to the denominator of the month before to be included
        in a conversion for this month. 
        These people are not "true" conversions but must be included for completeness. 
        TODO turn on ability for converting twice?

        Buckets begin at beginning of month with the first contributions,
        e.g. Earliest contribution is 2017-01-20 for Augur, first bucket is 2017-01-01 start

        :returns: dict[str, dict[str, int]] where they keys are timestamps and the values are 
            dictionaries of uuid: count of valid contributions 
        """
        numerators = {}
        denominators = {}
        converters = set()
        date_of_first_contribution_by_uuid = {}
        buckets_in_consideration_numerator = deque()  # Contains only data within a Lag Time behind.
        buckets_in_consideration_denominator = deque()  # Contains only data within a Lag Time behind.
        last_date_out_of_range_numerator = []  # This list will be empty until we have passed at least Lag Time months from start
        time_buckets = list(zip(self.time_buckets_denominator, self.time_buckets_numerator)) # Zip into tuple for shorter implementation
        
        for i, bucket in enumerate(time_buckets):
            # info: Use a current bucket (numerator or denominator does not matter) just to get the dates required
            bucket_start_date = parser.parse(bucket[0]['key_as_string'])
            prev_bucket_start_date = parser.parse(time_buckets[i-1][0]['key_as_string']) if i > 0 else None

            # info: Append the current bucket to the list of buckets in consideration
            if len(buckets_in_consideration_numerator) >= self.tracking_lag_period:
                assert len(buckets_in_consideration_numerator) == len(buckets_in_consideration_denominator)
                last_date_out_of_range_numerator = buckets_in_consideration_numerator.popleft()
                buckets_in_consideration_denominator.popleft()
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
            d1 = self.contributors_filtered_by_cutoff(cumulative_bucket_authors_denominator, self.d1_cutoff, self.d2_cutoff)  # TODO need this cutoff or else will double count in denominator
            denominators[bucket_start_date] = d1

            if i > 0:
                d2 = self.contributors_filtered_by_cutoff(last_date_out_of_range_numerator + cumulative_bucket_authors_numerator, 
                                                        self.d2_cutoff + 1,
                                                        None)

                # info: Initially, we filtered to include first time contributors only to prevent re-conversions.
                    # Now, it is allowed. TODO allow option to disallow?
                numerators[bucket_start_date] = d2

                first_time_D2 = dict(filter(lambda x: date_of_first_contribution_by_uuid[x[0]] == bucket_start_date,
                                                            d2.items()))
                denominators[prev_bucket_start_date].update(first_time_D2)

                converters.update(d2.keys())

        return numerators, denominators


    def calculate_cr_series(self, numerators, denominators, converters_all):
        """
        Calculate a series of conversion rates at fixed intervals by comparing contributors
        at different levels after a certain time interval.
        Reindex the resul for visualization.

        Numerators and denominators must be of length n and n+1 respectively,
        giving us n conversion rates.

        Numerators = people in level to (no duplicates within a given date)
        Denominators = people in level from (no duplicates within a given date)

        This calculation method takes the union of the numerators and denominators, 
        calculates its size and divides that by the size of the denominators.

        TODO test set difference

        Constraints: 
        - Time stamps must match with denominators consisting of 1 earlier time stamp and
        all others shared with numerators. 
        - Conversion rate is designated as 0 if there are no users in the denominator.

        Parallel bulk can be used to accelerate reindexing (see article by Lynn Kwong)

        :param numerators: Output from get_contributors (dict[datetime, dict[str, int]])
        :param denomiantors: Output from get_contributors (dict[datetime, dict[str, int]])
        :returns: None
        """
        print(len(denominators))
        print(len(numerators))
        assert len(denominators) - len(numerators) == 1 or len(denominators) == 0
        numerators = collections.OrderedDict(sorted(numerators.items()))
        denominators = collections.OrderedDict(sorted(denominators.items()))
        logging.info(f"Numerators length {len(numerators)} and denominators length {len(denominators)}")
        has_converted_before = set()
        
        timestamps = list(denominators.keys())

        # Step through timestamps as numerator timestamps (for denominator, look 1 interval backwards)
        for i, date in enumerate(timestamps[1:]):
            logging.info(f"Now calculating CR for {date} with {numerators[date]} and {denominators[timestamps[i]]}")

            # Find uuids who completed conversion (those which are shared between sets)
            converters = (numerators[date].keys() & denominators[timestamps[i]].keys()) - has_converted_before
            converters_all.append(converters)
            if not self.allow_multiple_conversions: 
                has_converted_before.update(converters) # Update "converted" set with current converters

            logging.info(f"By the end of month {date} these people converted {converters}")

            logging.info(f"Converters for {date} are {list(converters)}")

            doc = {
                "_index": self.out_index_name,
                "_type": "_doc",
                "_source": {
                    'date_of_conversion': date,
                    'conversion_rate': len(converters) / len(denominators[timestamps[i]])
                        if len(denominators[timestamps[i]]) else float(0),
                    'num_converters': int(len(converters)),
                    'converters': list(converters),
                    'repo_name': self.repo_name,
                    'project_name': self.project_name
                }
            }
            yield doc


if __name__ == '__main__':
    # CONF = yaml.safe_load(open('conf.yaml'))
    # print(elasticsearch.__version__)

    # # # Toggle these on for debug
    # # converters_all = []
    # # helpers.bulk(es, calculate_cr_series(d2, d1, converters_all)) # Bulk upload

    # conversion_rate_model = Aggregate(**CONF)
    # d2, d1 = conversion_rate_model.get_contributors()
    # converters_all = []
    # helpers.bulk(conversion_rate_model.es, conversion_rate_model.calculate_cr_series(d2, d1, converters_all))
    print(elasticsearch.__version__)
    pass