#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-21 21:29

import functools
import re
import time
import logging

import requests
from bs4 import BeautifulSoup

import proxy_switcher
from api_helper import CRAWLER_HEADERS
from token_bucket import Bucket
from utils.jsdati import JsdatiApi
from config.config import V2EX_USERNAME, V2EX_PASSWORD, JSDATI_USERNAME, JSDATI_PASSWORD
from utils.notification import wechat_notify

V2EX_INDEX_URL = 'https://www.v2ex.com'
V2EX_SIGNIN_URL = 'https://www.v2ex.com/signin'
V2EX_TOPIC_WEB_URL = 'https://www.v2ex.com/t/{topic_id}'

bucket = Bucket(rate=0.3, burst=1)

dmapi = JsdatiApi(JSDATI_USERNAME, JSDATI_PASSWORD)

def consume_token(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        while bucket.get() < 1:
            time.sleep(0.5)
        bucket.desc()
        return func(*args, **kwargs)
    return wrapper


class WebExtras(object):

    def __init__(self):
        self.subtle_list = []
        self.click = 0
        self.favorite = 0
        self.thank = 0


class WebHelper(object):
    """Web parser"""

    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers = CRAWLER_HEADERS

        self.click_regex = re.compile(r'\d+(?= 次点击)')
        self.favorite_regex = re.compile(r'\d+(?= 人收藏)')
        self.thank_regex = re.compile(r'\d+(?= 人感谢)')

    @consume_token
    def signin(self):
        if (not V2EX_USERNAME) or (not V2EX_PASSWORD) or (not JSDATI_USERNAME) or (not JSDATI_PASSWORD):
            logging.error('Missing username or password')
            return False

        if self.has_signined():
            return True

        logging.info('Signin to V2EX')
        proxy = proxy_switcher.random_proxy()
        signin_page_response = self.session.get(V2EX_SIGNIN_URL, proxies=proxy)

        if signin_page_response.status_code == 403:
            logging.error('Sign in 403')
            time.sleep(30)
            return False

        time.sleep(5)
        if not signin_page_response.text:
            return False

        soup = BeautifulSoup(signin_page_response.text, 'html5lib')
        login_form = soup.find('form', action='/signin')
        username_key = login_form.find('input', placeholder='用户名或电子邮箱地址')['name']
        password_key = login_form.find('input', type='password')['name']
        once_token = login_form.find('input', type='hidden', attrs={'name': 'once'})['value']
        captcha_key = login_form.find('input', placeholder='请输入上图中的验证码')['name']
        captcha_url = V2EX_INDEX_URL + '/_captcha?once=' + once_token

        image_bincontent = self.session.get(captcha_url).content
        captcha = dmapi.decode_image_bin_content(image_bincontent, 200)
        if (not isinstance(captcha, str)) or (not captcha):
            wechat_notify(once_token + '验证码打码失败')
            logging.warning('Decode captcha failed: ' + str(captcha))
            return False

        headers = CRAWLER_HEADERS.copy()
        headers['referer'] = V2EX_SIGNIN_URL

        payload = {username_key: V2EX_USERNAME, password_key: V2EX_PASSWORD,
                   'once': once_token, captcha_key: captcha, 'next': '/'}
        self.session.post(V2EX_SIGNIN_URL, payload, headers=headers, proxies=proxy)
        if self.has_signined():
            return True
        else:
            wechat_notify(once_token + '登录失败')
            return False

    @consume_token
    def has_signined(self):
        proxy = proxy_switcher.random_proxy()
        response = self.session.get(V2EX_INDEX_URL, proxies=proxy)
        if response.status_code != 200:
            return True
        index_page = response.text
        return 'class="top">登出</a></td>' in index_page

    @consume_token
    def get_topic_extras(self, topic_id) -> WebExtras:
        extras = WebExtras()
        proxy = proxy_switcher.random_proxy()
        logging.info('random proxy: ' + str(proxy))
        topic_page_response = None
        try:
            topic_page_response = self.session.get(V2EX_TOPIC_WEB_URL.format(topic_id=topic_id),
                                                   allow_redirects=False,
                                                   timeout=5, proxies=proxy)
        except Exception as e:
            logging.error('get_topic_extras error: ' + str(e))
            proxy_switcher.mute_random_proxy(proxy)

        if topic_page_response is None:
            return self.get_topic_extras(topic_id)

        if topic_page_response.status_code == 302:
            if not self.has_signined():
                self.signin()
                return self.get_topic_extras(topic_id)
            else:
                logging.info('New accounts can\'t access some topics and will get 302,'
                             ' change your V2EX account')
                return extras

        if topic_page_response.status_code == 404:
            return None

        if topic_page_response.status_code == 403:
            time.sleep(10)
            proxy_switcher.mute_random_proxy(proxy)
            logging.info('403 Access Denied')
            return self.get_topic_extras(topic_id)

        if topic_page_response.status_code != 200:
            logging.error('Something went wrong when fetch extras, status code:{0} '
                          'response:{1}'.format(topic_page_response.status_code, topic_page_response.text))
            time.sleep(2)
            proxy_switcher.mute_random_proxy(proxy)
            return self.get_topic_extras(topic_id)

        if 'class="top">登出</a></td>' not in topic_page_response.text:
            self.signin()
            return self.get_topic_extras(topic_id)

        soup = BeautifulSoup(topic_page_response.text, 'html5lib')
        subtle_divs = soup.find_all('div', attrs={'class': 'subtle'})
        for subtle_div in subtle_divs:
            content_div = subtle_div.find(attrs={'class': 'topic_content'})
            extras.subtle_list.append(content_div.get_text())
        statistics_div = soup.find('div', attrs={'class': 'fr topic_stats'})
        if statistics_div:
            """
            '2569 次点击  ∙  4 人收藏   ∙  1 人感谢   '
            '2569 次点击  '
            """
            text = statistics_div.get_text()
            click_regex_match = re.search(self.click_regex, text)
            if click_regex_match:
                extras.click = int(click_regex_match.group())

            favorite_regex_match = re.search(self.favorite_regex, text)
            if favorite_regex_match:
                extras.favorite = int(favorite_regex_match.group())

            thank_regex_match = re.search(self.thank_regex, text)
            if thank_regex_match:
                extras.thank = int(thank_regex_match.group())

        else:
            logging.critical('Something went wrong when parse statistics_div, status code:{0} '
                             'response:{1}'.format(topic_page_response.status_code, topic_page_response.text))
        return extras


if __name__ == '__main__':
    helper = WebHelper()
    helper.get_topic_extras(214424)

