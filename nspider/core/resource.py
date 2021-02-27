#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: resource.py
# @time: 2021.02.24 17:52
# @desc:


import requests
from .request import Request
from nspider.settings import Settings

class Resource(Request):
    def __init__(self, url: str, filename: str, save_dir: str, method="GET",
                 stream=True, verbose=False, try_num=Settings.RETRY_NUM,
                 fix_type=True, session=requests.Session(),
                 callback=None, headers=Settings.HEADERS, params={}, data={},
                 cookies={}, proxies={}, priority=0, errback=None):

        # 请求方法
        self.filename = filename
        self.save_dir = save_dir
        self.method = str(method).upper()
        self.stream = stream
        self.verbose = verbose
        self.try_num = try_num
        self.fix_type = fix_type
        self.session = session
        self.callback = callback
        self.headers = headers
        self.params = params
        self.data = data
        self.cookies = cookies
        self.proxies = proxies
        self.errback = errback
        # 优先级
        self.priority = priority

        # 设置url
        self._set_url(url)
        assert isinstance(priority, int), "Request priority not an integer: %r" % priority

        self.set_fingerprint()
