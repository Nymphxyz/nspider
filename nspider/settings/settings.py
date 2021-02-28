#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @license: (C) Copyright 2020-2020
# @contact: xyz.hack666@gmail.com
# @file: settings.py
# @time: 2020.11.09 16:22
# @desc:

#FIXME: 创建 settings 类

import os

"""
Request object: 3kb
response object: 
"""

class Settings(object):
    USER_AGENT = r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 ' \
                 r'Safari/537.36 '
    HEADERS = {'User-Agent': USER_AGENT}

    __RECOMMEND_MAX_WORKERS = os.cpu_count() * 3

    # Transactions Per Second
    TPS = 3

    # for non core thread
    KEEP_ALIVE_TIME_FOR_NON_CORE_THREAD = 10

    # the number of proxy thread
    CORE_PROXIER_WORKER_POOL_SIZE = 1
    MAX_PROXIER_WORKER_POOL_SIZE = 1

    # the number of fetcher thread
    CORE_FETCHER_WORKER_POOL_SIZE = 5
    MAX_FETCHER_WORKER_POOL_SIZE = 7
    RETRY_NUM = 2

    # the number of parser thread
    CORE_ANALYZER_WORKER_POOL_SIZE = 5
    MAX_ANALYZER_WORKER_POOL_SIZE = 7

    BUFFER_REQUEST_THRESHOLD = 1000

    DB = "spider.db"

    def recommend(self, tps):
        self.TPS = tps
        if self.TPS < self.__RECOMMEND_MAX_WORKERS:
            re_num = self.TPS
        else:
            re_num = self.__RECOMMEND_MAX_WORKERS

        self.CORE_PROXIER_WORKER_POOL_SIZE = re_num
        self.CORE_FETCHER_WORKER_POOL_SIZE = re_num
        self.CORE_ANALYZER_WORKER_POOL_SIZE = re_num

        self.MAX_PROXIER_WORKER_POOL_SIZE = re_num + 2
        self.MAX_FETCHER_WORKER_POOL_SIZE = re_num + 2
        self.MAX_ANALYZER_WORKER_POOL_SIZE = re_num + 2

        try:
            import psutil
            pc_mem = psutil.virtual_memory()
            div_gb_factor = (1024.0 ** 1)
            ava_mem = float(pc_mem.available / div_gb_factor)
        except:
            ava_mem = 60000
        ren = int((ava_mem / 20) / 3)
        if ren > 100000: ren = 100000
        self.BUFFER_REQUEST_THRESHOLD = ren
        self.auto_correct()
        self.check_validity()

    def check_validity(self):
        if self.KEEP_ALIVE_TIME_FOR_NON_CORE_THREAD <= 0:
            raise Exception("KEEP_ALIVE_TIME_FOR_NON_CORE_THREAD can't be negative")

        if self.TPS <= 0:
            raise Exception("TPS can't be negative")

        if self.CORE_PROXIER_WORKER_POOL_SIZE <= 0:
            raise Exception("CORE_PROXIER_WORKER_POOL_SIZE can't be 0 or negative")

        if self.CORE_FETCHER_WORKER_POOL_SIZE <= 0:
            raise Exception("CORE_FETCHER_WORKER_POOL_SIZE can't be 0 or negative")

        if self.CORE_ANALYZER_WORKER_POOL_SIZE <= 0:
            raise Exception("CORE_ANALYZER_WORKER_POOL_SIZE can't be 0 or negative")

        if self.BUFFER_REQUEST_THRESHOLD <= 0:
            raise Exception("BUFFER_REQUEST_THRESHOLD can't be 0 or negative")

        if self.MAX_PROXIER_WORKER_POOL_SIZE < self.CORE_PROXIER_WORKER_POOL_SIZE:
            raise Exception("Max proxier size shouldn't be smaller than core size")

        if self.MAX_FETCHER_WORKER_POOL_SIZE < self.CORE_FETCHER_WORKER_POOL_SIZE:
            raise Exception("Max fetcher size shouldn't be smaller than core size")

        if self.MAX_ANALYZER_WORKER_POOL_SIZE < self.CORE_ANALYZER_WORKER_POOL_SIZE:
            raise Exception("Max analyzer size shouldn't be smaller than core size")

    def auto_correct(self):
        if self.MAX_FETCHER_WORKER_POOL_SIZE < self.CORE_FETCHER_WORKER_POOL_SIZE:
            self.MAX_FETCHER_WORKER_POOL_SIZE = self.CORE_FETCHER_WORKER_POOL_SIZE

        if self.MAX_ANALYZER_WORKER_POOL_SIZE < self.CORE_ANALYZER_WORKER_POOL_SIZE:
            self.MAX_ANALYZER_WORKER_POOL_SIZE = self.CORE_ANALYZER_WORKER_POOL_SIZE
