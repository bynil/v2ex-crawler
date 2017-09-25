#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: gexiao
# Created on 2017-01-08 13:40

# A copy from pyspider of Binux: https://github.com/binux/pyspider
# Licensed under the Apache License, Version 2.0: https://github.com/binux/pyspider/blob/master/LICENSE

import time
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading


class Bucket(object):

    '''
    traffic flow control with token bucket
    '''

    update_interval = 30

    def __init__(self, rate=1, burst=None):
        self.rate = float(rate)
        if burst is None:
            self.burst = float(rate) * 10
        else:
            self.burst = float(burst)
        self.mutex = _threading.Lock()
        self.bucket = self.burst
        self.last_update = time.time()

    def get(self):
        '''Get the number of tokens in bucket'''
        now = time.time()
        if self.bucket >= self.burst:
            self.last_update = now
            return self.bucket
        bucket = self.rate * (now - self.last_update)
        self.mutex.acquire()
        if bucket > 1:
            self.bucket += bucket
            if self.bucket > self.burst:
                self.bucket = self.burst
            self.last_update = now
        self.mutex.release()
        return self.bucket

    def set(self, value):
        '''Set number of tokens in bucket'''
        self.bucket = value

    def desc(self, value=1):
        '''Use value tokens'''
        self.bucket -= value