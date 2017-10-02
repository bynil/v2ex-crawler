#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-10-03 01:08

import requests
from config.config import FTQQ_KEY


def wechat_notify(content):
    ftqq_url = 'http://sc.ftqq.com/{0}.send?text={1}&desp={2}'.format(FTQQ_KEY, 'V2EX 爬虫', content)
    requests.get(ftqq_url)

