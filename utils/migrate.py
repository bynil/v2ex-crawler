#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-09-08 15:28

import sys
sys.path.append('..')

import math
import pymongo
import json
import re
from datetime import datetime
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from config.config import *


SHARDS_NUMBER = 1
REPLICAS_NUMBER = 0

DEFAULT_SETTINGS = {"settings": {
    "index": {
        "number_of_shards": SHARDS_NUMBER,
        "number_of_replicas": REPLICAS_NUMBER,
        "refresh_interval": "30s"
    }
}}

TOPIC_MAX_BONUS = 15.0

TOPIC_INDEX_NAME = "topic_v1"

TOPIC_ALIAS_NAME = "topic"

POSTSCRIPT_MAPPING = {
    "mappings": {
        "postscript": {
            "_all": {
                "enabled": False
            },
            "dynamic": "strict",
            "properties": {
                "content": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                },
                "sequence": {
                    "type": "short"
                },
                "topic_id": {
                    "type": "integer"
                },
                "id": {
                    "type": "keyword"
                }
            }
        }
    }
}

TOPIC_MAPPING = {
    "mappings": {
        "topic": {
            "_all": {
                "enabled": False
            },
            "dynamic": "strict",
            "properties": {
                "click": {
                    "type": "integer"
                },
                "content": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                },
                "created": {
                    "type": "date"
                },
                "deleted": {
                    "type": "boolean"
                },
                "favorite": {
                    "type": "integer"
                },
                "id": {
                    "type": "integer"
                },
                "last_modified": {
                    "type": "date"
                },
                "last_touched": {
                    "type": "date"
                },
                "member": {
                    "type": "keyword"
                },
                "node": {
                    "type": "integer"
                },
                "replies": {
                    "type": "integer"
                },
                "thank": {
                    "type": "integer"
                },
                "title": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                }
            }
        }
    }
}

REPLY_MAPPING = {
    "mappings": {
        "reply": {
            "_all": {
                "enabled": False
            },
            "dynamic": "strict",
            "properties": {
                "content": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                },
                "created": {
                    "type": "date"
                },
                "deleted": {
                    "type": "boolean"
                },
                "id": {
                    "type": "integer"
                },
                "last_modified": {
                    "type": "date"
                },
                "member": {
                    "type": "keyword"
                },
                "thanks": {
                    "type": "integer"
                },
                "topic_id": {
                    "type": "integer"
                }
            }
        }
    }
}

ASSEMBLED_TOPIC_MAPPING = {
    "mappings": {
        "topic": {
            "dynamic": "strict",
            "_all": {
                "enabled": False
            },
            "properties": {
                "all_content": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                },
                "all_reply": {
                    "type": "text",
                    "analyzer": "ik_max_word"
                },
                "click": {
                    "type": "integer"
                },
                "content": {
                    "type": "text",
                    "copy_to": [
                        "all_content"
                    ],
                    "analyzer": "ik_max_word"
                },
                "created": {
                    "type": "date"
                },
                "deleted": {
                    "type": "boolean"
                },
                "favorite": {
                    "type": "integer"
                },
                "id": {
                    "type": "integer"
                },
                "last_modified": {
                    "type": "date"
                },
                "last_touched": {
                    "type": "date"
                },
                "member": {
                    "type": "keyword"
                },
                "node": {
                    "type": "integer"
                },
                "bonus": {
                    "type": "float"
                },
                "postscript_list": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "content": {
                            "type": "text",
                            "copy_to": [
                                "all_content"
                            ],
                            "analyzer": "ik_max_word"
                        },
                        "id": {
                            "type": "keyword"
                        },
                        "sequence": {
                            "type": "short"
                        },
                        "topic_id": {
                            "type": "integer"
                        }
                    }
                },
                "replies": {
                    "type": "integer"
                },
                "reply_list": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "content": {
                            "type": "text",
                            "copy_to": [
                                "all_content", "all_reply"
                            ],
                            "analyzer": "ik_max_word"
                        },
                        "created": {
                            "type": "date"
                        },
                        "deleted": {
                            "type": "boolean"
                        },
                        "id": {
                            "type": "integer"
                        },
                        "last_modified": {
                            "type": "date"
                        },
                        "member": {
                            "type": "keyword"
                        },
                        "reply_id": {
                            "type": "integer"
                        },
                        "thanks": {
                            "type": "integer"
                        },
                        "topic_id": {
                            "type": "integer"
                        }
                    }
                },
                "thank": {
                    "type": "integer"
                },
                "title": {
                    "type": "text",
                    "copy_to": [
                        "all_content"
                    ],
                    "analyzer": "ik_max_word"
                }
            }
        }
    }
}


image_url_pattern = re.compile(r"https?:\/\/[A-Za-z0-9_\-\/\.]+?\.(jpg|jpeg|gif|png)")


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def sub_image_url(content):
    if not content:
        return ""
    return re.sub(image_url_pattern, " ", content)


def filter_topic(topic, wanted_keys=TOPIC_MAPPING["mappings"]["topic"]["properties"].keys()):
    assert len(wanted_keys) > 0
    wanted_topic = {key: topic[key] for key in wanted_keys if key in topic}
    if "content" in wanted_topic:
        wanted_topic["content"] = sub_image_url(wanted_topic["content"])
    wanted_topic["reply_list"] = []
    wanted_topic["postscript_list"] = []
    return wanted_topic


def filter_reply(reply, wanted_keys=REPLY_MAPPING["mappings"]["reply"]["properties"].keys()):
    assert len(wanted_keys) > 0
    wanted_reply = {key: reply[key] for key in wanted_keys if key in reply}
    if "content" in wanted_reply:
        wanted_reply["content"] = sub_image_url(wanted_reply["content"])
    return wanted_reply


def filter_postscript(postscript, wanted_keys=POSTSCRIPT_MAPPING["mappings"]["postscript"]["properties"].keys()):
    wanted_postscript = {key: postscript[key] for key in wanted_keys if key in postscript}
    if "content" in wanted_postscript:
        wanted_postscript["content"] = sub_image_url(wanted_postscript["content"])
    wanted_postscript["id"] = str(wanted_postscript["topic_id"]) + "_" + str(wanted_postscript["sequence"])
    return wanted_postscript


def assemble_topic(topic, reply_collection, postscript_collection):
    topic_id = topic["id"]
    wanted_topic = filter_topic(topic)

    reply_cursor = reply_collection.find({"topic_id": topic_id}).sort("id", pymongo.ASCENDING)
    for reply in reply_cursor:
        wanted_reply = filter_reply(reply)
        wanted_topic["reply_list"].append(wanted_reply)

    postscript_cursor = postscript_collection.find({"topic_id": topic_id}).sort("sequence", pymongo.ASCENDING)
    for postscript in postscript_cursor:
        wanted_postscript = filter_postscript(postscript)
        wanted_topic["postscript_list"].append(wanted_postscript)

    if topic["deleted"]:
        bonus = 0.0
    else:
        favorite = int(topic["favorite"]) if "favorite" in topic else 0
        thank = int(topic["thank"]) if "thank" in topic else 0
        node_id = int(topic["node"])

        # log(1 + 5*count, 2)
        bonus = round(float(math.log2((1 + 5 * favorite) * (1 + 5 * thank))), 2)
        bonus = min(bonus, TOPIC_MAX_BONUS)

        # sandbox: 542, ohno: 983
        if node_id != 542 and node_id != 983:
            bonus += 5

    wanted_topic["bonus"] = bonus
    return wanted_topic


class Migrate(object):
    def __init__(self):
        if MONGODB_USER:
            self.client = MongoClient(MONGODB_HOST, MONGODB_PORT,
                                      username=MONGODB_USER, password=MONGODB_PASSWORD,
                                      authSource=MONGODB_DBNAME, authMechanism='SCRAM-SHA-1')
        else:
            self.client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        
        db = self.client.v2ex
        
        self.topic_collection = db.topic
        self.reply_collection = db.reply
        self.postscript_collection = db.postscript
        
        self.es = Elasticsearch([ES_HOST])

    def create_indices(self):
        self.es.indices.create(index=TOPIC_INDEX_NAME, body={**DEFAULT_SETTINGS, **ASSEMBLED_TOPIC_MAPPING}, ignore=400)

    def migrate_assembled_topics(self):
        search_body = {
            "aggs": {
                "max_id": {
                    "max": {
                        "field": "id"
                    }
                }
            },
            "size": 1
        }
        max_topic = self.es.search(index=TOPIC_INDEX_NAME, doc_type="topic", body=search_body)
        es_max_topic_id = max_topic["aggregations"]["max_id"]["value"]
        if not es_max_topic_id:
            es_max_topic_id = 0
        es_max_topic_id = int(es_max_topic_id)

        beginning_time = datetime.utcnow()
        cursor = self.topic_collection.find({"id": {"$gt": es_max_topic_id}}).sort("id", pymongo.ASCENDING)

        bulk_body = ""
        bulk_count = 0
        for topic in cursor:
            topic_id = topic["id"]
            wanted_topic = assemble_topic(topic, self.reply_collection, self.postscript_collection)
            single_doc_str = json.dumps({"index": {"_index": TOPIC_INDEX_NAME, "_type": "topic", "_id": topic_id}}) + \
                             "\n" + \
                             json.dumps(wanted_topic, default=json_serial) + \
                             "\n"
            bulk_body = bulk_body + single_doc_str
            bulk_count += 1
            if bulk_count >= 3000:
                print("Start inserting topic " + str(topic_id))
                self.es.bulk(body=bulk_body, index=TOPIC_INDEX_NAME, doc_type="topic", request_timeout=20)
                bulk_count = 0
                bulk_body = ""
                print("Inserted topic " + str(topic_id))

        if bulk_body:
            self.es.bulk(body=bulk_body, index=TOPIC_INDEX_NAME, doc_type="topic")

        modified_count = self.update_topics_synced_state(beginning_time, True)
        print("Modified topic count: " + str(modified_count))

    def update_topics_synced_state(self, latest_crawled, synced):
        if latest_crawled and isinstance(latest_crawled, datetime):
            return self.topic_collection.update_many({"crawled": {"$lt": latest_crawled},
                                                      "web_crawled": {"$lt": latest_crawled}},
                                                     {"$set": {"synced": synced}},
                                                     upsert=False).modified_count

    def create_aliases(self):
        if self.es.indices.exists_alias(name=TOPIC_ALIAS_NAME):
            existing_alias = self.es.indices.get_alias(name=TOPIC_ALIAS_NAME)
            body = {
                "actions": [
                ]
            }
            for index_name, alias in existing_alias.items():
                body["actions"].append({"remove": {"index": index_name, "alias": TOPIC_ALIAS_NAME}})
            body["actions"].append({"add": {"index": TOPIC_INDEX_NAME, "alias": TOPIC_ALIAS_NAME}})
            self.es.indices.update_aliases(body)
        else:
            self.es.indices.put_alias(TOPIC_INDEX_NAME, TOPIC_ALIAS_NAME)


__all__ = ["filter_topic", "filter_reply", "filter_postscript", "assemble_topic",
           "TOPIC_ALIAS_NAME"]

if __name__ == "__main__":
    migrate = Migrate()
    migrate.create_indices()
    migrate.migrate_assembled_topics()
    migrate.create_aliases()
