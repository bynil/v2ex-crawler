#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-10-02 23:55

import hashlib
import urllib.request
import urllib
import json
import base64
import requests
from config.config import DAMATU_USERNAME, DAMATU_PASSWORD


def md5str(str):  # md5加密字符串
    m = hashlib.md5(str.encode(encoding="utf-8"))
    return m.hexdigest()


def md5(byte):  # md5加密byte
    return hashlib.md5(byte).hexdigest()


class DamatuApi():
    ID = '51211'
    KEY = '6a59caa0a4a52112a01e2633a62e6368'
    HOST = 'http://api.dama2.com:7766/app/'

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def getSign(self, param=b''):
        return (md5(bytes(self.KEY, encoding="utf8") + bytes(self.username, encoding="utf8") + param))[:8]

    def getPwd(self):
        return md5str(self.KEY + md5str(md5str(self.username) + md5str(self.password)))

    def post(self, path, params={}):
        data = urllib.parse.urlencode(params).encode('utf-8')
        url = self.HOST + path
        response = urllib.request.Request(url, data)
        return urllib.request.urlopen(response).read()

    # 查询余额 return 是正数为余额 如果为负数 则为错误码
    def getBalance(self):
        data = {'appID': self.ID,
                'user': self.username,
                'pwd': dmt.getPwd(),
                'sign': dmt.getSign()
                }
        res = self.post('d2Balance', data)
        res = str(res, encoding="utf-8")
        jres = json.loads(res)
        if jres['ret'] == 0:
            return jres['balance']
        else:
            return jres['ret']

    # 上传验证码 参数filePath 验证码图片路径 如d:/1.jpg type是类型，查看http://wiki.dama2.com/index.php?n=ApiDoc.Pricedesc  return 是答案为成功 如果为负数 则为错误码
    def decode(self, filePath, type):
        f = open(filePath, 'rb')
        fdata = f.read()
        filedata = base64.b64encode(fdata)
        f.close()
        data = {'appID': self.ID,
                'user': self.username,
                'pwd': self.getPwd(),
                'type': type,
                'fileDataBase64': filedata,
                'sign': self.getSign(fdata)
                }
        res = self.post('d2File', data)
        res = str(res, encoding="utf-8")
        jres = json.loads(res)
        if jres['ret'] == 0:
            # 注意这个json里面有ret，id，result，cookie，根据自己的需要获取
            return (jres['result'])
        else:
            return jres['ret']

    # url地址打码 参数 url地址  type是类型(类型查看http://wiki.dama2.com/index.php?n=ApiDoc.Pricedesc) return 是答案为成功 如果为负数 则为错误码
    def decodeUrl(self, url, type):
        data = {'appID': self.ID,
                'user': self.username,
                'pwd': self.getPwd(),
                'type': type,
                'url': urllib.parse.quote(url),
                'sign': self.getSign(url.encode(encoding="utf-8"))
                }
        res = self.post('d2Url', data)
        res = str(res, encoding="utf-8")
        jres = json.loads(res)
        if jres['ret'] == 0:
            # 注意这个json里面有ret，id，result，cookie，根据自己的需要获取
            return (jres['result'])
        else:
            return jres['ret']

    # 报错 参数id(string类型)由上传打码函数的结果获得 return 0为成功 其他见错误码
    def reportError(self, id):
        data = {'appID': self.ID,
                'user': self.username,
                'pwd': dmt.getPwd(),
                'id': id,
                'sign': dmt.getSign(id.encode(encoding="utf-8"))
                }
        res = self.post('d2ReportError', data)
        res = str(res, encoding="utf-8")
        jres = json.loads(res)
        return jres['ret']

    def decode_image_bin_content(self, content, type):
        fdata = content
        filedata = base64.b64encode(fdata)
        data = {'appID': self.ID,
                'user': self.username,
                'pwd': self.getPwd(),
                'type': type,
                'fileDataBase64': filedata,
                'sign': self.getSign(fdata)
                }
        res = self.post('d2File', data)
        res = str(res, encoding="utf-8")
        jres = json.loads(res)
        if jres['ret'] == 0:
            # 注意这个json里面有ret，id，result，cookie，根据自己的需要获取
            return (jres['result'])
        else:
            return jres['ret']


if __name__ == '__main__':
    dmt = DamatuApi(DAMATU_USERNAME, DAMATU_PASSWORD)
    print(dmt.getBalance())
