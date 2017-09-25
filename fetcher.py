#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-12 15:24

import logging
from datetime import datetime
from api_helper import APIHelper
from data.data import DataManager
from web_helper import WebHelper
from elasticsearch import ElasticsearchException


class Fetcher(object):
    """API fetcher to complete tasks"""

    def __init__(self):
        self.api = APIHelper()
        self.data = DataManager()
        self.web = WebHelper()

    def fetch_single_topic(self, topic_id):
        topic_info = self.api.get_topic_info(topic_id)
        partial_member = self.data.member_of_topic(topic_info)
        stored_topic = self.data.find_topic(topic_id)
        new_topic = self.data.handle_topic(topic_info, topic_id)
        if stored_topic:
            new_topic['web_crawled'] = stored_topic['web_crawled']
        else:
            new_topic['web_crawled'] = datetime.fromtimestamp(0)
            new_topic['click'] = 0
            new_topic['favorite'] = 0
            new_topic['thank'] = 0

        self.data.update_topic_synced_state(new_topic['id'], False)
        self.data.upsert_topic(new_topic)
        self.data.upsert_member(partial_member)

    def fetch_new_topics(self):
        max_stored_topic_id = self.data.max_stored_topic_id
        topic_count = self.api.get_topic_count()
        if max_stored_topic_id >= topic_count:
            return
        for topic_id in range(max_stored_topic_id + 1, topic_count + 1):
            self.fetch_single_topic(topic_id)

    def fetch_replies_of_topic(self, topic_id):
        replies = self.api.get_replies(topic_id)
        if replies:
            self.data.update_topic_synced_state(topic_id, False)
            for reply in replies:
                partial_member = self.data.member_of_reply(reply)
                self.data.upsert_reply(self.data.handle_reply(reply, topic_id))
                self.data.upsert_member(partial_member)

    def fetch_new_replies(self):
        max_stored_topic_id = self.data.max_stored_topic_id
        max_stored_topic_id_of_reply = self.data.max_stored_topic_id_of_reply
        if max_stored_topic_id_of_reply == 0:
            need_refetch_max_topic = False
        else:
            topic = self.data.find_topic(max_stored_topic_id_of_reply)
            replies = self.data.find_all_replies(max_stored_topic_id_of_reply)
            need_refetch_max_topic = not (replies.count() == topic['replies'])
        for topic_id in range(max_stored_topic_id_of_reply + 1 - int(need_refetch_max_topic), max_stored_topic_id + 1):
            if self.data.find_topic(topic_id=topic_id):
                self.fetch_replies_of_topic(topic_id)

    def fetch_single_topic_extras(self, topic_id):
        def upsert_counts(click, favorite, thank):
            self.data.upsert_topic_extras(topic_id, click, favorite, thank)
            self.data.update_topic_synced_state(topic_id, False)
            count = self.data.update_topic_web_crawled(topic_id, datetime.utcnow())
            logging.info('Update topic {0} extras, count {1}'.format(topic_id, count))

        web_extra = self.web.get_topic_extras(topic_id)
        if not web_extra:
            count = self.data.update_topic_web_crawled(topic_id, datetime.utcnow())
            topic = self.data.find_topic(topic_id)
            if (not topic) or ('click' not in topic):
                upsert_counts(0, 0, 0)
            logging.info('Topic {0} extras is None, count {1}'.format(topic_id, count))
            return
        for index, postscript in enumerate(web_extra.subtle_list):
            postscript = self.data.handle_postscript(postscript, topic_id, index+1)
            self.data.upsert_postscript(postscript)

        upsert_counts(web_extra.click, web_extra.favorite, web_extra.thank)

    def fetch_all_topic_extras(self):
        while True:
            min_topic_id_need_postscript = self.data.min_topic_id_need_extras
            if min_topic_id_need_postscript > 0:
                try:
                    self.fetch_single_topic_extras(min_topic_id_need_postscript)
                except ElasticsearchException as es_error:
                    raise es_error
                except Exception as e:
                    logging.error('Fetch single topic extras error: ' + str(e))
            else:
                break

    def fetch_all_nodes(self):
        nodes = self.api.get_all_nodes()
        if nodes:
            for node in nodes:
                node['crawled'] = datetime.utcnow()
                self.data.upsert_node(node)

        logging.info('Fetching all node, count: ' + str(len(nodes)))

    def fetch_new_members(self):
        site_stats = self.api.get_site_stats()
        max_stored_member_id = self.data.max_stored_member_id
        if site_stats:
            member_max = site_stats['member_max']
            for member_id in range(max_stored_member_id+1, member_max+1):
                new_member = self.api.get_member_info(member_id=member_id)
                if not new_member:
                    continue
                new_member['crawled'] = datetime.utcnow()
                self.data.upsert_member(new_member)
                logging.info('Upsert member {0}, id {1}'.format(new_member['username'], new_member['id']))
        else:
            new_member = self.api.get_member_info(member_id=self.data.max_stored_member_id + 1)
            while new_member:
                new_member['crawled'] = datetime.utcnow()
                self.data.upsert_member(new_member)
                logging.info('Upsert member {0}, id {1}'.format(new_member['username'], new_member['id']))
                new_member = self.api.get_member_info(member_id=self.data.max_stored_member_id + 1)

    def fetch_stale_topics(self):
        stale_topics = self.data.stale_topics()
        if not stale_topics:
            return
        for topic in stale_topics:
            topic_id = topic['id']
            self.fetch_single_topic(topic_id)
            if topic['web_crawled'] and topic['recrawl'] and topic['web_crawled'] < topic['recrawl']:
                self.fetch_single_topic_extras(topic_id)
            self.fetch_replies_of_topic(topic_id)

    def sync_topic_to_es(self):
        waiting_topics = self.data.not_synced_topics()
        if not waiting_topics:
            return
        for topic in waiting_topics:
            self.data.update_topic_synced_state(topic['id'], True)
            self.data.es_update_assembled_topic(topic)


if __name__ == '__main__':
    fetcher = Fetcher()
    fetcher.fetch_single_topic_extras(289676)