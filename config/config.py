#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-09-22 22:47


MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
MONGODB_USER = ''
MONGODB_PASSWORD = ''
MONGODB_DBNAME = 'v2ex'

ES_HOST = '127.0.0.1:9200'

DAMATU_USERNAME = ''
DAMATU_PASSWORD = ''

# shadowsocks
# ('server_host', 'server_port', 'encrypt_method', 'password')
ss_config_list = []

V2EX_USERNAME = ''
V2EX_PASSWORD = ''

FTQQ_KEY = ''

__all__ = ['ES_HOST', 'MONGODB_HOST', 'MONGODB_PORT',
           'MONGODB_DBNAME', 'MONGODB_PASSWORD', 'MONGODB_USER',
           'ss_config_list', 'V2EX_USERNAME', 'V2EX_PASSWORD']
