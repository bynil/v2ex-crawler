#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-10 21:30


import time
import pymongo
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from pymongo import MongoClient, collection
from config.config import *
from utils.migrate import *

RECRAWL_DELTA_DAYS = [1, 5, 30]


class DataManager(object):
    def __init__(self):
        if MONGODB_USER:
            client = MongoClient(MONGODB_HOST, MONGODB_PORT,
                                 username=MONGODB_USER, password=MONGODB_PASSWORD,
                                 authSource=MONGODB_DBNAME, authMechanism='SCRAM-SHA-1')
        else:
            client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client.v2ex
        self.es = Elasticsearch([ES_HOST])
        self.node_collection = db.node
        self.topic_collection = db.topic
        self.reply_collection = db.reply
        self.postscript_collection = db.postscript
        self.member_collection = db.member
        self.proxy_collection = db.proxy
        self.create_indexes()

    def create_indexes(self):
        self.node_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.node_collection.create_index([('name', pymongo.ASCENDING)])

        self.topic_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.topic_collection.create_index([('member_name', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.topic_collection.create_index([('node', pymongo.ASCENDING)])
        self.topic_collection.create_index([('created', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.topic_collection.create_index([('last_touched', pymongo.ASCENDING)])
        self.topic_collection.create_index([('deleted', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.topic_collection.create_index([('web_crawled', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.topic_collection.create_index([('recrawl', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.topic_collection.create_index([('synced', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])

        self.postscript_collection.create_index([('topic_id', pymongo.ASCENDING),
                                                 ('sequence', pymongo.ASCENDING)], unique=True)

        self.reply_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.reply_collection.create_index([('member_name', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.reply_collection.create_index([('topic_id', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
        self.reply_collection.create_index([('created', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])

        self.member_collection.create_index([('id', pymongo.ASCENDING)], unique=True)
        self.member_collection.create_index([('username', pymongo.ASCENDING)])
        self.member_collection.create_index([('created', pymongo.ASCENDING)])

        self.proxy_collection.create_index([('ip', pymongo.ASCENDING)], unique=True)

    # Node
    def upsert_node(self, node):
        node_id = node.get('id')
        if not node_id:
            return None
        return self.node_collection.find_one_and_replace({'id': node_id}, node, upsert=True,
                                                         return_document=collection.ReturnDocument.AFTER)

    def insert_nodes(self, node_list):
        return self.node_collection.insert_many(node_list)

    def find_node(self, node_id, name, object_id):
        if node_id:
            return self.node_collection.find_one({'id': node_id})
        if name:
            return self.node_collection.find_one({'name': name})
        if object_id:
            return self.node_collection.find_one({'_id': object_id})
        return None

    @property
    def max_stored_node_id(self):
        last_node_result = self.node_collection.find(limit=1).sort('id', pymongo.DESCENDING)
        if last_node_result.count() > 0:
            return last_node_result[0]['id']
        else:
            return 0

    # Topic
    @staticmethod
    def handle_topic(topic, topic_id):
        recrawl = datetime(2050, 1, 1)
        if topic is None:
            topic = {'id': topic_id, 'deleted': True,
                     'crawled': datetime.utcnow(), 'recrawl': recrawl}
            return topic

        assert (topic['member'])
        assert (topic['node'])
        assert ('created' in topic)
        assert ('last_modified' in topic)
        assert ('last_touched' in topic)

        topic['member'] = topic['member']['username']
        topic['node'] = topic['node']['id']
        topic['created'] = datetime.utcfromtimestamp(topic['created'])
        topic['last_modified'] = datetime.utcfromtimestamp(topic['last_modified'])
        topic['last_touched'] = datetime.utcfromtimestamp(topic['last_touched'])
        topic['deleted'] = False
        topic['crawled'] = datetime.utcnow()
        topic['synced'] = False

        now = datetime.utcnow()
        created_time = topic['created']
        delta = now - created_time
        days = delta.days
        for delta_days in RECRAWL_DELTA_DAYS:
            if days < delta_days:
                recrawl = created_time + timedelta(days=delta_days)
                break

        topic['recrawl'] = recrawl
        return topic

    # Partial Member
    @staticmethod
    def member_of_topic(topic):
        if topic:
            return topic['member']
        return None

    # Partial Member
    @staticmethod
    def member_of_reply(reply):
        if reply:
            return reply['member']
        return None

    def upsert_topic(self, topic):
        topic_id = topic.get('id')
        if not topic_id:
            return None
        return self.topic_collection.update_one({'id': topic_id}, {'$set': topic}, upsert=True).modified_count

    def es_update_assembled_topic(self, topic):
        wanted_topic = assemble_topic(topic, self.topic_collection, self.postscript_collection)
        body = {"doc": wanted_topic, "doc_as_upsert": True, "detect_noop": False}
        return self.es.update(index=TOPIC_ALIAS_NAME, doc_type='topic', id=topic['id'], body=body)

    def find_topic(self, topic_id=None, object_id=None):
        if topic_id:
            return self.topic_collection.find_one({'id': topic_id})
        if object_id:
            return self.topic_collection.find_one({'_id': object_id})
        return None

    def update_topic_web_crawled(self, topic_id, update_time):
        return self.topic_collection.update_one({'id': topic_id},
                                                {'$set': {'web_crawled': update_time}},
                                                upsert=True).modified_count

    def update_topic_synced_state(self, topic_id, synced):
        return self.topic_collection.update_one({'id': topic_id},
                                                {'$set': {'synced': synced}},
                                                upsert=True).modified_count

    def upsert_topic_extras(self, topic_id, click=0, favorite=0, thank=0):
        return self.topic_collection.update_one({'id': topic_id},
                                                {'$set': {'click': click, 'favorite': favorite, 'thank': thank}},
                                                upsert=True).modified_count

    def stale_topics(self):
        topics = self.topic_collection.find({'recrawl': {'$lt': datetime.utcnow()}}).sort('id', pymongo.ASCENDING)
        return topics

    def not_synced_topics(self):
        topics = self.topic_collection.find({'synced': False}).sort('id', pymongo.ASCENDING)
        return topics

    @property
    def min_topic_id_need_extras(self):
        min_datetime = datetime(2017, 1, 1, 0, 0, 0)
        last_topic_result = self.topic_collection.find({'web_crawled': {'$lt': min_datetime}},
                                                       limit=1).sort('id', pymongo.ASCENDING)
        if last_topic_result.count() > 0:
            return last_topic_result[0]['id']
        else:
            return 0

    @property
    def max_stored_topic_id(self):
        last_topic_result = self.topic_collection.find(limit=1).sort('id', pymongo.DESCENDING)
        if last_topic_result.count() > 0:
            return last_topic_result[0]['id']
        else:
            return 0

    # Reply
    @staticmethod
    def handle_reply(reply, topic_id):
        assert (reply['member'])
        assert ('content' in reply)
        assert ('created' in reply)
        assert ('last_modified' in reply)

        reply['member'] = reply['member']['username']
        reply['topic_id'] = topic_id
        reply['created'] = datetime.utcfromtimestamp(reply['created'])
        reply['last_modified'] = datetime.utcfromtimestamp(reply['last_modified'])
        reply['deleted'] = False
        reply['crawled'] = datetime.utcnow()

        return reply

    def upsert_reply(self, reply):
        reply_id = reply.get('id')
        if not reply_id:
            return None
        return self.reply_collection.find_one_and_replace({'id': reply_id}, reply, upsert=True,
                                                          return_document=collection.ReturnDocument.AFTER)

    def find_reply(self, reply_id=None, object_id=None):
        if reply_id:
            return self.reply_collection.find_one({'id': reply_id})
        if object_id:
            return self.reply_collection.find_one({'_id': object_id})
        return None

    def find_all_replies(self, topic_id):
        return self.reply_collection.find({'topic_id': topic_id})

    @property
    def max_stored_reply_id(self):
        last_reply_result = self.reply_collection.find(limit=1).sort('id', pymongo.DESCENDING)
        if last_reply_result.count() > 0:
            return last_reply_result[0]['id']
        else:
            return 0

    @property
    def max_stored_topic_id_of_reply(self):
        reply = self.reply_collection.find(limit=1).sort('topic_id', pymongo.DESCENDING)
        if reply.count() > 0:
            return reply[0]['topic_id']
        else:
            return 0

    # Postscript
    @staticmethod
    def handle_postscript(postscript, topic_id, sequence):
        return {'topic_id': topic_id,
                'content': postscript,
                'sequence': sequence,
                'crawled': datetime.utcnow()}

    def upsert_postscript(self, postscript):
        topic_id = postscript.get('topic_id')
        sequence = postscript.get('sequence')
        if not topic_id:
            return None
        return self.postscript_collection.find_one_and_replace({'topic_id': topic_id, 'sequence': sequence},
                                                               postscript, upsert=True,
                                                               return_document=collection.ReturnDocument.AFTER)

    # Member
    def upsert_member(self, member):
        if not member:
            return None
        member_id = member.get('id')
        if not member_id:
            return None
        return self.member_collection.update_one({'id': member_id}, {'$set': member},
                                                 upsert=True).modified_count

    @property
    def max_stored_member_id(self):
        members = self.member_collection.find(limit=1).sort('id', pymongo.DESCENDING)
        if members.count() > 0:
            return members[0]['id']
        else:
            return 0

    # Proxy IP
    """
    {
        'ip': '117.121.26.221',
        'reset_time': 1484377200
    }
    """

    def upsert_ip(self, ip_doc):
        return self.proxy_collection.find_one_and_replace({'ip': ip_doc['ip']}, ip_doc, upsert=True)

    def find_ip(self, ip):
        return self.proxy_collection.find_one({'ip': ip})

    def find_limited_ips(self):
        result = self.proxy_collection.find({'reset_time': {'$gt': int(time.time())}})
        limited_ip_list = []
        for ip in result:
            limited_ip_list.append(ip['ip'])
        return limited_ip_list


if __name__ == '__main__':
    data = DataManager()
    topic = data.find_topic(391977)
    result = data.es_update_assembled_topic(topic)
    print(result)
