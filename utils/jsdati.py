#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2018-05-07 22:04

import requests
import base64

SERVER_HOST = 'https://v2-api.jsdama.com/upload'
SOFTWARE_ID = 9487
SOFTWARE_SECRET = 'nb4GHmdsPxzbcB7iIrU36JPI73HOjUyUEnq3pkob'


class JsdatiApi():

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def decode_image_bin_content(self, content, type):
        filedata = base64.b64encode(content).decode('ascii')
        payload = {'softwareId': SOFTWARE_ID,
                   'softwareSecret': SOFTWARE_SECRET,
                   'username': self.username,
                   'password': self.password,
                   'captchaData': filedata,
                   'captchaType': 1017,  # 8位或8位以上英文或数字类型
                   }

        headers = {
            'Accept-Encoding': "application/json, text/javascript, */*; q=0.01",
            'Content-Type': "application/json",
        }

        response = requests.request("POST", SERVER_HOST, json=payload, headers=headers)
        res = response.json()
        # {"code":0,"data":{"recognition":"NDSBJCSY","captchaId":"20180507:000000000016483190234"},"message":""}
        if res['code'] == 0:
            return res['data']['recognition']
        else:
            return res['code']