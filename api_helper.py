#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-09 20:53

import time
import requests
import functools
import proxy_switcher
import logging
from token_bucket import Bucket
from config.config import V2EX_USERNAME

CRAWLER_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Upgrade-Insecure-Requests': "1",
    'User-Agent': 'V2EX Crawler @{username}'.format(username=V2EX_USERNAME),
    'pragma': 'no-cache',
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
}

# V2EX API
V2EX_SITE_URL = 'https://www.v2ex.com'

STATS_API_PATH = '/api/site/stats.json'
ALL_NODES_PATH = '/api/nodes/all.json'
NODE_INFO_PATH = '/api/nodes/show.json'  # param: `id` or `name`
LATEST_TOPICS_PATH = '/api/topics/latest.json'
TOPIC_INFO_PATH = '/api/topics/show.json'  # param: `id`
REPLIES_OF_TOPIC_PATH = '/api/replies/show.json'  # param: `topic_id`
MEMBER_INFO_PATH = '/api/members/show.json'  # param: `id` or `username`

API_RATE_LIMIT_ONE_HOUR = 120

bucket = Bucket(rate=0.5, burst=1)


def consume_token(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        while bucket.get() < 1:
            time.sleep(0.5)
        bucket.desc()
        return func(*args, **kwargs)
    return wrapper


class APIHelper(object):

    """API service with traffic flow controller"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers = CRAWLER_HEADERS

    @consume_token
    def _send_request(self, path, params=None):
        """

        :param path: path
        :param params: Dictionary
        :return: JSON object or None
        """

        url = V2EX_SITE_URL + path
        # proxy = proxy_switcher.get_proxy()
        try:
            proxy = proxy_switcher.random_proxy()

            def do_request():
                p = {}
                if proxy:
                    p = proxy.copy()
                response = self.session.get(url, params=params, timeout=60, proxies=p)
                logging.info('do request with proxy {proxy}'.format(proxy=proxy))
                limit_remain = int(response.headers.get('x-rate-limit-remaining', API_RATE_LIMIT_ONE_HOUR))
                if response.status_code == 403:
                    logging.info('api response 403')
                    limit_remain = 0
                logging.info('limit remain: {0}'.format(str(limit_remain)))
                if limit_remain <= 1:
                    reset_time = int(response.headers.get('x-rate-limit-reset', int(time.time()+1800)))
                    proxy_switcher.tag_current_ip_limited(reset_time)
                    proxy_switcher.mute_random_proxy(proxy)
                    return response if limit_remain else None
                else:
                    return response

            valid_response = None
            retry_times = 0
            while not valid_response:
                try:
                    if retry_times >= 5:
                        logging.info('Mark ip as limited because of too many retry times')
                        proxy_switcher.tag_current_ip_limited(int(time.time() + 120))
                        proxy_switcher.mute_random_proxy(proxy)
                        proxy = proxy_switcher.random_proxy()
                    retry_times += 1
                    valid_response = do_request()
                except requests.exceptions.RequestException as e:
                    if isinstance(e, requests.exceptions.ReadTimeout) and path == REPLIES_OF_TOPIC_PATH:
                        # ignore timeout for some large topics: https://www.v2ex.com/t/991748
                        # TODO refactor by API V2
                        logging.warning(
                            'ReadTimeout Error when fetch {url} {params} with proxy{proxy}: {exception}, ignore replies'
                            .format(proxy=proxy, url=url, params=params, exception=str(e)))
                        return None

                    logging.error('Error when fetch {url} {params} with proxy{proxy}: {exception}'
                                  .format(proxy=proxy, url=url, params=params, exception=str(e)))
                    self.session = requests.Session()
                    self.session.headers = CRAWLER_HEADERS
                    if retry_times >= 5:
                        logging.info('Mark ip as limited because of too many retry times')
                        proxy_switcher.tag_current_ip_limited(int(time.time() + 120))
                        proxy_switcher.mute_random_proxy(proxy)
                        proxy = proxy_switcher.random_proxy()

            return valid_response.json(strict=False)

        except ValueError:
            logging.critical('Error when parse json for {url} params {params}'.format(url=url, params=params))
            raise

    def get_topic_count(self):

        """
        Get the max number of topic id
        """

        latest_topics = self._send_request(LATEST_TOPICS_PATH)
        return int(latest_topics[0]['id'])

    def get_all_nodes(self):
        """

        [
          {
            "id": 1,
            "name": "babel",
            "url": "http://www.v2ex.com/go/babel",
            "title": "Project Babel",
            "title_alternative": "Project Babel",
            "topics": 1119,
            "header": "Project Babel - 帮助你在云平台上搭建自己的社区",
            "footer": "V2EX 基于 Project Babel 驱动。Project Babel 是用 Python 语言写成的，运行于 Google App Engine 云计算平台上的社区软件。Project Babel 当前开发分支 2.5。最新版本可以从 <a href=\"http://github.com/livid/v2ex\" target=\"_blank\">GitHub</a> 获取。",
            "created": 1272206882
          }
        ]
        """
        return self._send_request(ALL_NODES_PATH)

    def get_topic_info(self, topic_id):
        """

        [
          {
            "id": 74249,
            "title": "[V2EX技巧] 贴图和贴代码的方法（写给像我一样的小白）",
            "url": "http://www.v2ex.com/t/74249",
            "content": "一、贴图\r\n\r\n使用各种图片空间保存图片，比如：http://www.v2ex.com/t/44453?p=2\r\n\r\n然后复制图片地址，注意地址后缀是图片后缀（比如.jpg）才行\r\n\r\n发帖时直接放图片网址即可，不用任何其他代码\r\n\r\n\r\n二、贴代码\r\n\r\n这个我刚才试验了半天才搞明白\r\n\r\n在这里保存代码 https://gist.github.com/ 这个大家一看就会用\r\n\r\n生成链接后复制左上第一个链接\r\n\r\n格式是这个样子的 https://gist.github.com/5894938.git\r\n\r\n这时候要把 https 修改为 http，并去掉最后的 .git\r\n\r\n然后在发帖时直接粘贴修改后的链接即可。\r\n\r\n\r\n\r\n\r\n分享一下，给像我一样的新手",
            "content_rendered": "一、贴图<br /><br />使用各种图片空间保存图片，比如：http://www.v2ex.com/t/44453?p=2<br /><br />然后复制图片地址，注意地址后缀是图片后缀（比如.jpg）才行<br /><br />发帖时直接放图片网址即可，不用任何其他代码<br /><br /><br />二、贴代码<br /><br />这个我刚才试验了半天才搞明白<br /><br />在这里保存代码 <a href=\"https://gist.github.com/\" rel=\"nofollow\">https://gist.github.com/</a> 这个大家一看就会用<br /><br />生成链接后复制左上第一个链接<br /><br />格式是这个样子的 <a href=\"https://gist.github.com/5894938.git\" rel=\"nofollow\">https://gist.github.com/5894938.git</a><br /><br />这时候要把 https 修改为 http，并去掉最后的 .git<br /><br />然后在发帖时直接粘贴修改后的链接即可。<br /><br /><br /><br /><br />分享一下，给像我一样的新手",
            "replies": 239,
            "member": {
              "id": 37826,
              "username": "alay9999",
              "tagline": "签名是啥东西？我试试，就是试试啊~",
              "avatar_mini": "//cdn.v2ex.co/avatar/f570/1b02/37826_mini.png?m=1366456119",
              "avatar_normal": "//cdn.v2ex.co/avatar/f570/1b02/37826_normal.png?m=1366456119",
              "avatar_large": "//cdn.v2ex.co/avatar/f570/1b02/37826_large.png?m=1366456119"
            },
            "node": {
              "id": 300,
              "name": "programmer",
              "title": "程序员",
              "url": "http://www.v2ex.com/go/programmer",
              "topics": 14421,
              "avatar_mini": "//cdn.v2ex.co/navatar/94f6/d7e0/300_mini.png?m=1483659584",
              "avatar_normal": "//cdn.v2ex.co/navatar/94f6/d7e0/300_normal.png?m=1483659584",
              "avatar_large": "//cdn.v2ex.co/navatar/94f6/d7e0/300_large.png?m=1483659584"
            },
            "created": 1372594522,
            "last_modified": 1372594522,
            "last_touched": 1477605667
          }
        ]
        """
        result = self._send_request(TOPIC_INFO_PATH, {'id': topic_id})
        if isinstance(result, list):
            if len(result) == 1:
                topic = result[0]
                return topic
            else:
                return None
        else:
            raise ValueError('Get topic info error')

    def get_replies(self, topic_id):
        """

        [
          {
            "id": 3934452,
            "thanks": 0,
            "content": "竟然有入口！！说好的没有呢！这样价值就很大了！",
            "content_rendered": "竟然有入口！！说好的没有呢！这样价值就很大了！",
            "member": {
              "id": 154072,
              "username": "kulove",
              "tagline": "程序猿一枚，偶尔写写故事、搞搞安全。",
              "avatar_mini": "//cdn.v2ex.co/avatar/0fe8/6033/154072_mini.png?m=1465412458",
              "avatar_normal": "//cdn.v2ex.co/avatar/0fe8/6033/154072_normal.png?m=1465412458",
              "avatar_large": "//cdn.v2ex.co/avatar/0fe8/6033/154072_large.png?m=1465412458"
            },
            "created": 1483925298,
            "last_modified": 1483925298
          },
        ]
        """
        reply_list = self._send_request(REPLIES_OF_TOPIC_PATH, {'topic_id': topic_id})
        return reply_list if isinstance(reply_list, list) else None

    def get_member_info(self, member_id=None, username=None):
        """

        {
            "status": "found",
            "id": 50494,
            "url": "http://www.v2ex.com/member/mornlight",
            "username": "mornlight",
            "website": "",
            "twitter": "gexiao94",
            "psn": "",
            "github": "Bynil",
            "btc": "",
            "location": "Shanghai",
            "tagline": "",
            "bio": "",
            "avatar_mini": "//v2ex.assets.uxengine.net/avatar/4668/c712/50494_mini.png?m=1475389082",
            "avatar_normal": "//v2ex.assets.uxengine.net/avatar/4668/c712/50494_normal.png?m=1475389082",
            "avatar_large": "//v2ex.assets.uxengine.net/avatar/4668/c712/50494_large.png?m=1475389082",
            "created": 1385355037
        }
        """
        if member_id:
            member = self._send_request(MEMBER_INFO_PATH, {'id': member_id})
        elif username:
            member = self._send_request(MEMBER_INFO_PATH, {'username': username})
        else:
            return None
        return member if member['status'] == 'found' else None

    def get_site_stats(self):
        """
        {
            "topic_max": 378676,
            "member_max": 250084
        }
        """
        return self._send_request(STATS_API_PATH)
