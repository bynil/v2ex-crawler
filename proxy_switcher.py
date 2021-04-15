#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-13 18:27

import time
import shlex
import logging
import subprocess
import os
from random import randint
from config.config import ss_config_list, http_proxy_list
from data.data import DataManager

## subprocess.call(('killall', 'ss-local'))

current_ip = ''
local_ip = 'local_ip'

data_manager = DataManager()

ss_process_list = []
ss_proxy_list = []
random_proxy_mute_dict = {}


def create_ss_proxies():
    global ss_process_list, ss_proxy_list
    if ss_process_list:
        return

    proxies_env = os.getenv('HTTP_PROXY_LIST')
    if proxies_env:
        proxies = proxies_env.split(',')
        for p in proxies:
            ss_proxy_list.append(p)

    for http_proxy in http_proxy_list:
        ss_proxy_list.append(http_proxy)

    logging.info('ss_proxy_list ' + str(ss_proxy_list))
    for index, config in enumerate(ss_config_list):
        local_port = 1090 + index
        args = shlex.split(
            'ss-local -s {0} -p {1} -l {local_port} '
            '-k {3} -m {2} -t 10 -b 0.0.0.0'.format(local_port=local_port, *config))
        p = subprocess.Popen(args)
        ss_process_list.append(p)
        ss_proxy_list.append('127.0.0.1:' + str(local_port))
        if index == len(ss_config_list) - 1:
            time.sleep(2)


def close_all_ss_local():
    for ss_process in ss_process_list:
        ss_process.terminate()
        ss_process.wait()


def auto_choose_ip():
    limited_ip_list = data_manager.find_limited_ips()
    if current_ip and current_ip not in limited_ip_list:
        return current_ip

    # if local_ip not in limited_ip_list:
    #     return local_ip

    for ss_proxy in ss_proxy_list:
        if ss_proxy not in limited_ip_list:
            return ss_proxy

    return ''


def get_proxy():
    valid_ip = auto_choose_ip()
    global current_ip
    if current_ip != valid_ip:
        logging.info('Change IP to ' + valid_ip)
    current_ip = valid_ip

    if not current_ip:
        logging.info('No valid ip, go to sleep')
        time.sleep(300)
        return get_proxy()

    if current_ip == local_ip:
        return {}

    # TODO
    if 'https://' in current_ip:
        return {'https': current_ip}
    if '127.0.0.1:' in current_ip:
        return {'https': 'socks5://{ss_proxy}'.format(ss_proxy=current_ip)}
    if '10.0.' in current_ip:
        return {'https': 'http://{ss_proxy}'.format(ss_proxy=current_ip)}
    return {}


def random_proxy():
    def new_random_proxy():
        random_index = randint(0, len(ss_proxy_list)-1)
        random_ip = local_ip
        if random_index < len(ss_proxy_list):
            random_ip = ss_proxy_list[random_index]

        if random_ip == local_ip:
            return {}

        if '127.0.0.1:' in random_ip:
            return {'https': 'socks5://{ss_proxy}'.format(ss_proxy=random_ip)}
        if '10.0.' in random_ip:
            return {'https': 'http://{ss_proxy}'.format(ss_proxy=random_ip)}
        return {'https': '{ss_proxy}'.format(ss_proxy=random_ip)}

    def test_all_proxies():
        if len(random_proxy_mute_dict) < len(ss_proxy_list)+1:
            return
        for proxy, mute_time in random_proxy_mute_dict.items():
            if mute_time <= time.time():
                return
        logging.info('All proxies are muted')
        time.sleep(200)

    proxy = new_random_proxy()

    if str(proxy) in random_proxy_mute_dict:
        mute_time = random_proxy_mute_dict[str(proxy)]
        if mute_time > time.time():
            test_all_proxies()
            return random_proxy()
        else:
            del random_proxy_mute_dict[str(proxy)]

    return proxy


def mute_random_proxy(proxy, second=3600):
    if proxy is not None:
        random_proxy_mute_dict[str(proxy)] = time.time() + second
        logging.info('mute proxy: ' + str(proxy))


def tag_current_ip_limited(reset_time):
    global current_ip
    data_manager.upsert_ip({'ip': current_ip, 'reset_time': reset_time})
    current_ip = ''


