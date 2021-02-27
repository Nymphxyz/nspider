#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: request.py
# @time: 2020.11.19 16:45
# @desc:

import requests
from hashlib import md5

from nspider.settings import Settings


class Request(object):
    def __init__(self, url: str, parser_class, method="GET", session=requests.Session(), callback=None, headers=Settings.HEADERS, params={}, data={},
                 cookies={}, proxies={}, priority=0, errback=None):

        # 请求方法
        self.method = str(method).upper()
        self.parser_class = parser_class # class not object
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

    @property
    def fingerprint(self):
        return self.__fingerprint

    def set_fingerprint(self):
        self.__fingerprint = self.__generate_request_fingerprint()

    def _set_url(self, url):
        if not isinstance(url, str):
            raise TypeError('Request url must be str, got %s:' % type(url).__name__)
        self.url = url

    #TODO: 检查url的合法性

    def __generate_request_fingerprint(self):
        """
            Return the request fingerprint.

            The request fingerprint is a hash that uniquely identifies the resource the
            request points to. For example, take the following two urls:

            http://www.example.com/query?id=111&cat=222
            http://www.example.com/query?cat=222&id=111

            Even though those are two different URLs both point to the same resource
            and are equivalent (ie. they should return the same response).
        """
        # get query from url
        parse = requests.utils.urlparse(self.url)
        # parse this query
        params_in_url = {}
        if parse.query:
            params_in_url = dict(x.split('=') for x in parse.query.split('&'))
        all_params = {**params_in_url, **self.params, **self.data}

        md5_obj = md5()
        md5_obj.update(self.method.encode(encoding='utf-8'))  # get/post
        md5_obj.update(parse.netloc.encode(encoding='utf-8'))  # "localhost"
        md5_obj.update(parse.path.encode(encoding='utf-8'))  # "/xxx/xxx.html"
        for key in sorted(all_params):
            md5_obj.update(all_params.get(key, "").encode(encoding='utf-8'))
        md5_url = md5_obj.hexdigest()
        return md5_url
