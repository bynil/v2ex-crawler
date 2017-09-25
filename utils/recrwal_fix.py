#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-09-22 22:53


import sys
sys.path.append('..')

import pymongo
from datetime import timedelta
from data.data import DataManager, RECRAWL_DELTA_DAYS


if __name__ == "__main__":
    data = DataManager()
    cursor = data.topic_collection.find({'id': {'$gt': 0}}).sort('id', pymongo.ASCENDING)
    for topic in cursor:
        if topic['deleted']:
            continue
        created_time = topic['created']
        crawled_time = topic['crawled']
        delta = crawled_time - created_time
        days = delta.days
        for delta_days in RECRAWL_DELTA_DAYS:
            if days < delta_days:
                recrawl = created_time + timedelta(days=delta_days)
                data.topic_collection.update_one({'id': topic['id']},
                                                 {'$set': {'recrawl': recrawl}},
                                                 upsert=True)
                print('Fix {topic_id} recrawl to {time}'.format(topic_id=topic['id'], time=recrawl.isoformat()))
                break