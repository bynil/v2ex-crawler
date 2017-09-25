#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-08 13:26

import sys
import traceback
import proxy_switcher
import threading
import time
import requests
import logging
from fetcher import Fetcher
from elasticsearch import ElasticsearchException
from config.config import V2EX_USERNAME, V2EX_PASSWORD


def notify(content):
    ftqq_url = 'http://sc.ftqq.com/SCU210Tc7bd35817825a00aecc8e' \
               'adde7a42ed5593fd5e35e0a6.send?text={0}&desp={1}'.format('V2EX 爬虫', content)
    requests.get(ftqq_url)


def init_logging():
    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        level=logging.INFO)


if __name__ == '__main__':
    init_logging()

    proxy_switcher.create_ss_proxies()
    try:
        fetcher = Fetcher()


        def fetch_extras():
            if (not V2EX_USERNAME) or (not V2EX_PASSWORD):
                logging.error('Missing v2ex username or password, topic extras will not be crawled')
                return
            while True:
                try:
                    fetcher.fetch_all_topic_extras()
                except ElasticsearchException as es_error:
                    notify('Elasticsearch error')
                    logging.critical('Elasticsearch error')
                    logging.critical(str(es_error))
                    logging.critical(traceback.format_exc())
                    raise es_error
                except Exception as e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())
                time.sleep(600)


        postscript_thread = threading.Thread(target=fetch_extras)
        postscript_thread.setDaemon(True)
        postscript_thread.start()


        def update_all_nodes():
            fetcher.fetch_all_nodes()


        while True:
            try:
                fetcher.fetch_new_topics()
                fetcher.fetch_new_replies()
                fetcher.fetch_new_members()
                fetcher.fetch_stale_topics()
                fetcher.sync_topic_to_es()
            except ElasticsearchException as es_error:
                notify('Elasticsearch error')
                logging.critical('Elasticsearch error')
                logging.critical(str(es_error))
                logging.critical(traceback.format_exc())
                raise es_error
            except Exception as e:
                logging.error(str(e))
                logging.error(traceback.format_exc())
            else:
                logging.info('All things go well, sleep 600s')
            time.sleep(600)

    except KeyboardInterrupt:
        proxy_switcher.close_all_ss_local()
        sys.exit(0)
