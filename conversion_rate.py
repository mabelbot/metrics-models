#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2022 Yehu Wangi, Chenqi Shan, Mabel Furutsuki
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Yehu Wang <yehui.wang.mdh@gmail.com>
#     Chenqi Shan <chenqishan337@gmail.com>
#     Mabel Furutsuki <mfurutsuki@berkeley.edu>


from datetime import datetime, timedelta
import json
import yaml
import pandas as pd
from pandas import json_normalize
import ssl, certifi
import requests
import hashlib
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan
from elasticsearch import helpers
import logging

from perceval.backend import uuid
from grimoire_elk.elastic_mapping import Mapping as BaseMapping  # TODO is this import right?
from grimoire_elk.enriched.utils import get_time_diff_days
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime,
                                          datetime_to_utc)
from grimoire_elk.elastic import ElasticSearch
import sortinghat.exceptions
from sortinghat import api
from sortinghat.db.database import Database
from sortinghat.matcher import create_identity_matcher

from metric_model import MetricsModel
import metric_model  # myc to import utils at the top - todo will move utils into their own module later
import identities

import aggregate

db = Database('root', '', 'test_sh')  # TODO

MAX_BULK_UPDATE_SIZE = 100  # TODO why does this not match the bulk size in elastic.py?

logging.basicConfig(filename='conversion_rate.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.info('Begin conversion_rate.py')

"""
class ConversionRateMetricsModel: 
    Inherits the following methods from MetricsModel
        - metrics_model_metrics(self)
        - metrics_model_enrich(repos_list, label)
        - get_uuid_count_query(self, option, repos_list, field, date_field="grimoire_creation_date", from_date=str_to_datetime("1970-01-01"), to_date= datetime_utcnow())
        - get_uuid_count_contribute_query(self, project, company=None, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow())
        - get_created_since_query(self, repo, order="asc")
        - get_updated_since_query(self, repository_url, date)
        - get_issue_closed_uuid_count(self, option, repos_list, field, from_date=str_to_datetime("1970-01-01"), to_date= datetime_utcnow())
    Inherits the following from metric_model
        - def get_date_list(begin_date, end_date, freq='W-MON'):...
        - def get_all_repo(file, source):...
        - def get_all_project(file):...
        - def get_time_diff_months(start, end):...

TODO: for more actions like commits, more fields might play a role
"""


class ConversionRateMetricsModel(MetricsModel):
    def __init__(self, **kwargs):
        """Metrics Model is designed for the integration of multiple CHAOSS metrics. This is the Conversion Rate Metric Model.
        The output is going to be a conversion rate with new contributors during tracking period P and sustained contributors
        assessed at a later fixed time T. User can specify the length of period P in days, a start date for all tracking S,
        and the lag time after period P when sustained contributors should be addressed L.
        new contributors tracking period should be at in days. T is a rolling time stamp.
        - Example: P = 60, S = 1970-01-01, L = 90. Means that we will track new contributors over 60 days starting from 1970-01-01,
            then, assess how many and who of those contributors has become a sustained contributor by the deadline of the next 90 days (60+90 = 150 days since the start). This is datapoint 1. Datapoint 2 will be tracking new contributors over the NEXT 60 days after the 60 days from datapoint 1, and so on until there are no more intervals possible.
        - Elasticsearch schema should include 1 line per contribution with contributor info, contribution type and time.
            - The conversion rate can be figured out from this with python calculation. Kibana can also be used to visualize
        - There is another schema we can use that has the results from different periods, only sustained contributors, etc.

            :params json_file, out_index, community, level: See superclass
            :param int tracking_period_length: length of tracking period P for new contributors (days)
            :param str github_index: name of the index that contains the grimoirelab-elk github.py enriched data (mixed issues/prs)
            :param str githubql_index: name of the index that contains the grimoirelab-elk githubql.py enriched data (mixed issues/prs)

         """
        super().__init__(kwargs.get('general').get('json_file'),
                         kwargs.get('general').get('out_index_base'),
                         kwargs.get('community'),
                         kwargs.get('level'))  # TODO change back to out index
        self.tracking_period_length = kwargs.get('tracking_period_length')
        self.from_date = kwargs.get('tracking-params').get('from_date')
        self.level = kwargs.get('general').get('level')
        logging.info(f'Analyzing at level: {self.level}')

        self.run_combine = kwargs.get('general').get('run_combine')
        self.out_index_base = kwargs.get('general').get('out_index_base')
        self.data_sources = kwargs.get('general').get('data_sources')

        self.github_repos = {} # Used for filtering repos by what user specifies

        self.github_index = kwargs.get('github-params').get('github_index')
        self.githubql_index = kwargs.get('github-params').get('githubql_index')
        self.github2_issues_enriched_index = kwargs.get('github-params').get('github2_issues_enriched_index')
        self.github2_pull_enriched_index = kwargs.get('github-params').get('github2_pull_enriched_index')
        self.combined_users = set()
        # self.issue_index = issue_index
        # self.repo_index = repo_index
        # self.git_index = git_index
        # self.git_branch = git_branch
        # self.date_list = get_date_list(from_date, end_date)
        # self.all_project = get_all_project(self.json_file)
        # self.all_repo = get_all_repo(self.json_file, self.issue_index.split('_')[0])

    def countributor_count(self, date, repos_list):
        query_author_uuid_data = self.get_uuid_count_contribute_query(repos_list, company=None,
                                                                      from_date=(date - timedelta(days=90)),
                                                                      to_date=date)
        author_uuid_count = \
            self.es_in.search(index=(self.git_index, self.issue_index), body=query_author_uuid_data)['aggregations'][
                "count_of_contributors"]['value']
        return author_uuid_count

    def countributor_count_D2(self, date, repos_list):
        query_author_uuid_data = self.get_uuid_count_contribute_query(repos_list, company=None,
                                                                      from_date=(date - timedelta(days=90)),
                                                                      to_date=date)
        author_uuid_count = \
            self.es_in.search(index=(self.git_index), body=query_author_uuid_data)['aggregations'][
                "count_of_contributors"][
                'value']
        return author_uuid_count

    def commit_frequence(self, date, repos_list):
        query_commit_frequency = self.get_uuid_count_query("cardinality", repos_list, "hash", "grimoire_creation_date",
                                                           from_date=date - timedelta(days=90), to_date=date)
        commit_frequency = self.es_in.search(index=self.git_index, body=query_commit_frequency)[
            'aggregations']["count_of_uuid"]['value']
        return commit_frequency

    def created_since(self, date, repos_list):
        updated_since_list = []
        for repo in repos_list:
            query_first_commit_since = self.get_created_since_query(repo, order="asc")
            first_commit_since = self.es_in.search(index=self.git_index, body=query_first_commit_since)['hits']['hits']
            if len(first_commit_since) > 0:
                creation_since = first_commit_since[0]['_source']["grimoire_creation_date"]
                updated_since_list.append(get_time_diff_months(creation_since, str(date)))
        if updated_since_list:
            return sum(updated_since_list) / len(updated_since_list)
        else:
            return 0

    def closed_issue_count(self, date, repos_list):
        query_issue_closed = self.get_issue_closed_uuid_count("cardinality", repos_list, "uuid",
                                                              from_date=(date - timedelta(days=90)), to_date=date)
        issue_closed = \
            self.es_in.search(index=self.issue_index, body=query_issue_closed)['aggregations']["count_of_uuid"]['value']
        return issue_closed

    def updated_issue_count(self, date, repos_list):
        query_issue_updated_since = self.get_uuid_count_query("cardinality", repos_list, "uuid",
                                                              from_date=(date - timedelta(days=90)), to_date=date)
        updated_issues_count = \
            self.es_in.search(index=self.issue_index, body=query_issue_updated_since)['aggregations']["count_of_uuid"][
                'value']
        return updated_issues_count

    def metrics_model_enrich(self, label, out_index):
        """Wrapper for methods metrics_model_combine_indexes_github and metrics_models_combine_github2_prs """
        logging.info(f'Entered metrics_model_enrich method with label {label} and out_index {out_index}')
        es_out = ElasticSearch(elastic_url, index=out_index, mappings=Mapping(), clean=True)
        self.metrics_model_combine_indexes_github(label, out_index)
        self.metrics_models_combine_github2_prs(label, out_index)

    def metrics_model_combine_indexes_github(self, label, out_index):  # todo figure out what to use for repos_list if it's the same index
        """
        Appends the necessary info from post-enrichment github_index to the existing 
        post-enrichment githubql_index for an index of contributions only. 
        At this point the repos might be mixed depending on what grimoirelab has been run with. 
        Issues/prs are also mixed. 

        The bulk_upload definition is contained in grimoire_elk/elastic.py.
            with the following method structure: def bulk_upload(self, items, field_id)

        Sortinghat UUIDs are different for githubql and github (and other data sources potentially). 
        We are going to try to standardize them. 
            - First use code in identities module to collapse all identities into one per github username 
            - Then, lookup reverse username from unique identity uuid during processing the index documents.
            - Then, for github, we'll switch the uuid to the merged one by a reverse lookup.

        Use Actor ID to track events.
        """
        es_out = ElasticSearch(elastic_url, index=out_index, mappings=Mapping(), clean=False)
        item_datas = []
        logging.info('Begin at combine indexes')

        # Append Github data for Event Creation Contributions (this contains PR and issue both) --------------
        search = scan(self.es_in,
                    index=self.github_index,
                    query={"query": {"match_all": {}}}
                    )
        hits = []

        for hit in search:
            hits.append(hit)

        for hit in hits:
            hit = json_normalize(hit).to_dict(orient='records')[0]

            if hit['_source.repository'] in self.github_repos:  # Only process repos user has specified for analysis
                # Combine SortingHat uuids for Github w/ Githubql ones
                if hit['_source.user_login'] not in self.combined_users:
                    identities.combine_identities(hit['_source.user_login'], ['github', 'githubql', 'github2']) #info: This is only relevant if an identity is present in both GH and GHQL
                    self.combined_users.add(hit['_source.user_login'])
                    logging.info(f"Finished combine for this user - {hit['_source.user_login']}")
                
                metrics_data = {
                    # SHARED FIELDS 
                    # '_index': hit['_index'],
                    # '_type': hit['_type'],
                    # '_id': hit['_id'],
                    # '_score': hit['_score'],
                    'sort': hit['sort'],
                    'metadata__updated_on': hit['_source.metadata__updated_on'],
                    'metadata__timestamp': hit['_source.metadata__timestamp'],
                    'offset': hit['_source.offset'],
                    'origin': hit['_source.origin'],
                    'tag': hit['_source.tag'],
                    'uuid': hit['_source.uuid'],
                    'repository': hit['_source.repository'],
                    'author_bot': hit['_source.author_bot'],
                    'author_domain': hit['_source.author_domain'],
                    # '_source.author_gender': 
                    # '_source.author_gender_acc': #TODO see if missing fields can be handled
                    'actor_id': str(api.search_unique_identities(db=db, term=hit['_source.user_login'])[0].uuid),
                    # TODO : used to be github username
                    'actor_username': hit['_source.user_login'],  # githubql necessary field
                    'author_multi_org_names': hit['_source.author_multi_org_names'],
                    'author_name': hit['_source.author_name'],
                    'author_org_name': hit['_source.author_org_name'],
                    'author_user_name': hit['_source.author_user_name'],
                    'author_uuid': hit['_source.author_uuid'],
                    'github_repo': hit['_source.github_repo'],
                    'grimoire_creation_date': hit['_source.grimoire_creation_date'],
                    'issue_url': hit['_source.issue_url'],
                    'item_type': hit['_source.item_type'],
                    'metadata__enriched_on': hit['_source.metadata__enriched_on'],
                    'metadata__filter_raw': hit['_source.metadata__filter_raw'],
                    'metadata__gelk_backend_name': hit['_source.metadata__gelk_backend_name'],
                    'metadata__gelk_version': hit['_source.metadata__gelk_version'],
                    'project': hit['_source.project'],
                    'project_1': hit['_source.project_1'],
                    'pull_request': hit['_source.pull_request'],
                    'title': hit['_source.title'],
                    'title_analyzed': hit['_source.title_analyzed'],
                    # TODO deal with assignee 2 way data and other stuff. first we deal with creation contributions

                    # Assign a mock "event type" field for creation events
                    'event_type': 'CreatedPREvent' if hit['_source.pull_request'] else 'CreatedEvent',
                    # githubql necessary field
                    'created_at': hit['_source.created_at'],
                    # githubql necessary field - this is the one we'll use for identifying time
                    'mode_level': label,
                    'associated_project': self.github_repos[hit['_source.repository']]
                }
                item_datas.append(metrics_data)
                if len(item_datas) >= MAX_BULK_UPDATE_SIZE:  # TODO >= or else you lose an item right?
                    es_out.bulk_upload(item_datas, 'uuid')  # TODO which field to use here?
                    item_datas = []

        es_out.bulk_upload(item_datas, 'uuid')
        item_datas = []

        # Append Github2 data ----------------------------------------------------------------------------------------
        search_github2 = scan(self.es_in,
                            index=self.github2_issues_enriched_index,
                            query={"query": {"match_all": {}}}
                            )
        hits_github2 = []

        for hit in search_github2:
            hits_github2.append(hit)

        for hit in hits_github2:
            hit = json_normalize(hit).to_dict(orient='records')[0]

            # TODO it appears that the value in this field is either 1.0 or nan, meaning the issues themselves are the "nan" (which we want to remove)
            # For example https://github.com/chaoss/augur/issues/302
            if '_source.is_github_issue_comment' not in hit or not hit['_source.is_github_issue_comment']: continue

            if hit['_source.repository'] in self.github_repos:  # Only process repos user has specified for analysis
                # Combine SortingHat uuids but only if they have not been combined before to save time
                if hit['_source.user_login'] not in self.combined_users:
                    identities.combine_identities(hit['_source.user_login'], ['github', 'githubql',
                                                                            'github2'])  # info: This is only relevant if an identity is present in both GH and GHQL
                    self.combined_users.add(hit['_source.user_login'])
                    logging.info(f"Finished combine for this user - {hit['_source.user_login']}")

                try:
                    user_uuid = str(api.search_unique_identities(db=db, term=hit['_source.user_login'])[0].uuid)
                except sortinghat.exceptions.NotFoundError:
                    print(f"Processing person {hit['_source.user_login']}")
                    user_uuid = hit['_source.user_login']

                metrics_data = {
                    # SHARED FIELDS  # TODO remove duplicates (issue again, but comments are new - scenario)
                    'sort': hit['sort'],
                    'metadata__updated_on': hit['_source.metadata__updated_on'],
                    'metadata__timestamp': hit['_source.metadata__timestamp'],
                    'tag': hit['_source.tag'],  # Perceval tag
                    'uuid': hit['_source.uuid'] + str(hashlib.md5(hit['_source.url'].encode()).hexdigest()),  # Perceval UUID
                    'repository': hit['_source.repository'],  # Repository Name
                    'author_bot': hit['_source.author_bot'],
                    # If author is a bot (also have user_data_bot and assignee_data_bot tbd)
                    'author_domain': hit['_source.author_domain'],  # Authors domain
                    'actor_id': user_uuid,  # Fill in actor with author's info
                    'actor_username': hit['_source.user_login'],  # githubql necessary field # user_data_user_name
                    'author_name': hit['_source.author_name'],
                    'author_org_name': hit['_source.author_org_name'],
                    'author_user_name': hit['_source.author_user_name'],
                    'author_uuid': hit['_source.author_uuid'],
                    # This is the old UUID from Sorting Hat that may not be combined
                    'github_repo': hit['_source.github_repo'],
                    'grimoire_creation_date': hit['_source.grimoire_creation_date'],
                    'issue_url': hit['_source.url'],  # Full url of issue including anchor
                    'item_type': hit['_source.item_type'],  # The type of the item, in this case it's (issue/comment).
                    'metadata__enriched_on': hit['_source.metadata__enriched_on'],
                    'metadata__filter_raw': hit['_source.metadata__filter_raw'],
                    'metadata__gelk_backend_name': hit['_source.metadata__gelk_backend_name'],
                    'metadata__gelk_version': hit['_source.metadata__gelk_version'],
                    'project': hit['_source.project'],  # Project name
                    'project_1': hit['_source.project_1'],
                    # Used if more than one project levels are allowed in the project hierarchy.
                    'pull_request': hit['_source.issue_pull_request'],  # Boolean for if this is a PR or not
                    'title': hit['_source.issue_title'],
                    # 'title_analyzed': hit['_source.issue_title_analyzed'], # This column doesn't exist
                    # TODO deal with assignee 2 way data and other stuff. first we deal with creation contributions

                    # Assign a mock "event type" field for creation events
                    'event_type': 'UpdatedCommentOnIssueEvent' if hit[
                        '_source.is_github_issue_comment'] else "RedundantIssue",
                    # githubql necessary field TODO are we missing any issues
                    'created_at': hit['_source.comment_updated_at'],
                    'comment_body': hit['_source.body'],
                    'mode_level': label,
                    'associated_project': self.github_repos[hit['_source.repository']]
                }

                item_datas.append(metrics_data)
                if len(item_datas) >= MAX_BULK_UPDATE_SIZE:  # TODO >= or else you lose an item right?
                    es_out.bulk_upload(item_datas, 'uuid')  # TODO which field to use here?
                    item_datas = []

        es_out.bulk_upload(item_datas, 'uuid')
        item_datas = []

        # Append Githubql data as-is ----------------------------------------------------------------------------------
        search = scan(self.es_in,
                    index=self.githubql_index,
                    query={"query": {"match_all": {}}}
                    )

        for hit in search:
            del hit["_id"]
            del hit['_type']
            del hit['_index']
            hit['mode_level']: label
            hit['associated_project']: self.github_repos[hit['_source']['repository']]
            item_datas.append(dict(hit)["_source"])
            if len(item_datas) >= MAX_BULK_UPDATE_SIZE:
                es_out.bulk_upload(item_datas, 'uuid')  # TODO see above
                item_datas = []

        es_out.bulk_upload(item_datas, 'uuid')
        item_datas = []

    def metrics_models_combine_github2_prs(self, label, out_index):
        """ 
        Purpose: Append data from github2 for comments on pull requests to output (combined) index.

        Actual pull requests (creation) are already accounted for by the github backend.
        So they are ignored here, the purpose is to find the comments.

        is_github_review_comment field is not available on all documents in the index (TODO)

        For Pull Requests, we have the following types of comments
        - review_comment

        user_login is the person who posted the comment, comment_updated_at, comment_created_at.
        
        :param self: self
        :returns: None (in place modification only)
        """
        es_out = ElasticSearch(elastic_url, index=out_index, mappings=Mapping(), clean=False)
        item_datas = []
        logging.info('Begin method: metrics_models_combine_github2_prs')
        # test_set = set()

        search_github2 = scan(self.es_in,
                              index=self.github2_pull_enriched_index,
                              query={"query": {"match": {"item_type": "comment"}}}
                              # TODO: review_comment is the only type right
                              )
        hits_github2 = []

        for hit in search_github2:
            # print(hit['_source']['sub_type'])
            hits_github2.append(hit)

        logging.info(f'Method metrics_models_combine_github2_prs number of hits {len(hits_github2)}')

        for hit in hits_github2:
            hit = json_normalize(hit).to_dict(orient='records')[0]

            # The following fields WILL vary depending on the schema of each backend
            # There should be only comments here

            if hit['_source.repository'] in self.github_repos:  # Only process repos user has specified for analysis
                # Combine SortingHat uuids but only if they have not been combined before to save time
                if hit['_source.user_login'] not in self.combined_users:
                    identities.combine_identities(hit['_source.user_login'], ['github', 'githubql',
                                                                              'github2'])  # info: This is only relevant if an identity is present in both GH and GHQL (TODO replace w instance var)
                    self.combined_users.add(hit['_source.user_login'])
                    logging.info(f"Finished combine for this user - {hit['_source.user_login']}")

                # Make sure there is a pull request associated with this comment
                assert hit['_source.pull_id'] and hit['_source.is_github_review_comment'] == 1

                try:
                    user_uuid = str(api.search_unique_identities(db=db, term=hit['_source.user_login'])[0].uuid)
                except sortinghat.exceptions.NotFoundError:
                    print(f"Processing person from github2 PR Comments {hit['_source.user_login']}")
                    user_uuid = hit['_source.user_login']

                # test_set.add(hit['_source.url'])

                metrics_data = {
                    # SHARED FIELDS
                    'sort': hit['sort'],
                    'metadata__updated_on': hit['_source.metadata__updated_on'],
                    'metadata__timestamp': hit['_source.metadata__timestamp'],
                    'origin': hit['_source.origin'],
                    'tag': hit['_source.tag'],  # Perceval tag
                    'uuid': hit['_source.uuid'] + str(hashlib.md5(hit['_source.url'].encode()).hexdigest()),  # Perceval UUID + md5 hash
                    'repository': hit['_source.repository'],  # Repository Link
                    'author_bot': hit['_source.author_bot'],
                    # If author is a bot (also have user_data_bot and assignee_data_bot tbd)
                    'author_domain': hit['_source.author_domain'],  # Authors domain
                    'actor_id': user_uuid,  # Fill in actor with author's info
                    'actor_username': hit['_source.user_login'],  # githubql necessary field # user_data_user_name
                    'author_name': hit['_source.author_name'],
                    'author_org_name': hit['_source.author_org_name'],
                    'author_user_name': hit['_source.author_user_name'],
                    'author_uuid': hit['_source.author_uuid'], # This is the old UUID from Sorting Hat that may not be combined
                    'github_repo': hit['_source.github_repo'],
                    'grimoire_creation_date': hit['_source.grimoire_creation_date'], # Main Date field	Pull request/comment creation date.
                    'issue_url': hit['_source.url'],  # Full url to the PR's comment including anchor
                    'item_type': hit['_source.sub_type'], # Type of item (PR (review) comment)
                    'metadata__enriched_on': hit['_source.metadata__enriched_on'],
                    'metadata__filter_raw': hit['_source.metadata__filter_raw'],
                    'metadata__gelk_backend_name': hit['_source.metadata__gelk_backend_name'],
                    'metadata__gelk_version': hit['_source.metadata__gelk_version'],
                    'project': hit['_source.project'],  # Project name
                    'project_1': hit['_source.project_1'], # Used if more than one project levels are allowed in the project hierarchy.
                    'pull_request': False,  # Boolean for if this is a PR or not
                    'title': hit['_source.issue_title'],
                    'event_type': 'UpdatedCommentOnPREvent' if hit['_source.sub_type'] == 'review_comment' else "OtherPRComment",  # githubql necessary field
                    'created_at': hit['_source.comment_updated_at'],
                    'comment_body': hit['_source.body'],
                    'mode_level': label,
                    'associated_project': self.github_repos[hit['_source.repository']]
                }

                item_datas.append(metrics_data)
                if len(item_datas) >= MAX_BULK_UPDATE_SIZE:
                    logging.info(f'Bulk Uploading {len(item_datas)} items')
                    es_out.bulk_upload(item_datas, 'uuid')  # TODO correct field to use
                    item_datas = []

        es_out.bulk_upload(item_datas, 'uuid')
        item_datas = []
        # print(len(test_set))

    def metrics_model_metrics(self):
        """
        TODO figure out what the superclass method does usually with the json file (this is projects.json)
        overrides superclass method metrics_model_metrics
        """
        self.es_in = Elasticsearch(elastic_url, use_ssl=False, verify_certs=False,
                                   connection_class=RequestsHttpConnection)  # TODO does use_ssl have to be True?
        # self.github_es_out = ElasticSearch(elastic_url, index=self.out_index_base, mappings=Mapping(), clean=True)

        # Depending on type of community , choose one or more of these levels
        # Community level is under construction -------------
        # if self.level == "community":  # communities would take care of whole repo and SIG under organization
        #     all_repos_list = self.all_repo
        #     label = "community"
        #     self.metrics_model_enrich(all_repos_list, self.community)

        # Analyze multiple repositories at once under the label of project
        if self.level == "project": 
            label = "project"
            all_repo_json = json.load(open(self.json_file))
            for project in all_repo_json:
                repos_list = []
                for j in all_repo_json[project]:
                    if j in self.data_sources:
                        repos_list.append(all_repo_json[project][j][0])
                        self.github_repos[all_repo_json[project][j][0]] = project # Add to  dict[repo link str, project name str] 
                self.metrics_model_enrich(label, self.out_index_base + "_" + project)
                # Place aggregate code here

        # Enriches all repos provided, each considered individually
        if self.level == "repo": 
            label = "repo"
            all_repo_json = json.load(open(self.json_file))
            repos_list = []
            for project in all_repo_json:
                for j in all_repo_json[project]:
                    if j in self.data_sources:
                        repos_list.append(all_repo_json[project][j][0]) 
                        # In repository case, the dictionary is 1 item long only
                        if self.run_combine:
                            logging.info("Running combine")
                            self.github_repos = {all_repo_json[project][j][0] : project}  # dict[repo link str, project name str]
                            self.metrics_model_enrich(label, self.out_index_base + "_" + all_repo_json[project][j][0].split('/')[-1])
                        
                        args = [all_repo_json[project][j][0].split('/')[-1], project, all_repo_json[project][j][0].split('/')[-1]]
                        # Calculate actual conversion rate by aggregation
                        aggregate_conversion_rate = aggregate.Aggregate(*args, **CONF)
                        d2, d1 = aggregate_conversion_rate.get_contributors()
                        converters_all = []
                        helpers.bulk(aggregate_conversion_rate.es, aggregate_conversion_rate.calculate_cr_series(d2, d1, converters_all))
            

class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.
        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = '''
             {
                "dynamic":true,
                "properties": {
                    "actor_id": {
                        "type": "keyword"
                    }
                }
            }
            '''

        return {"items": mapping}


if __name__ == '__main__':
    CONF = yaml.safe_load(open('conf.yaml'))
    elastic_url = CONF['url']
    tracking_kwargs = CONF['tracking-params']
    github_kwargs = CONF['github-params']
    general_kwargs = CONF['general']
    conversion_rate_model = ConversionRateMetricsModel(**CONF)
    conversion_rate_model.metrics_model_metrics()
    logging.info("Exit main method")

