#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: shared_memory_handler.py
# @time: 2021.02.10 17:41
# @desc:

import requests
from multiprocessing import JoinableQueue, Queue

import nspider.utilities.constant as const
from nspider.settings import Settings
from nspider.core.request import Request


class SharedMemoryHandler(object):
    def __init__(self, settings: Settings):
        # proxy queue
        self.__proxy_queue = JoinableQueue(maxsize=settings.MAX_PROXIER_WORKER_POOL_SIZE * 2)

        # request task queue
        self.__request_queue = JoinableQueue(maxsize=settings.MAX_FETCHER_WORKER_POOL_SIZE * 2)

        # reply form fetcher
        self.__request_reply_queue = Queue(-1)

        # request buffer task queue
        self.__request_buffer_queue = Queue(-1)

        # 爬取的 html content queue
        self.__parse_data_queue = JoinableQueue(maxsize=settings.MAX_ANALYZER_WORKER_POOL_SIZE * 2)

        self.log_message_queue = Queue(-1)

    def __add_request(self, queue, *args, new_request=None, block=True, timeout=None, **kwargs):
        if not new_request:
            new_request = Request(*args, **kwargs)
        queue.put(new_request, block, timeout)
        return True

    # request
    def add_request(self, *args, **kwargs):
        return self.__add_request(self.__request_queue, *args, **kwargs)

    def add_request_in_buffer(self, *args, **kwargs):
        self.__request_buffer_queue.put((args, kwargs))

    def add_request_reply(self, reply, block=True, timeout=None):
        self.__request_reply_queue.put(reply, block, timeout)

    def get_request_reply(self, block=True, timeout=None):
        return self.__request_reply_queue.get(block, timeout)

    def get_request(self, block=True, timeout=None):
        return self.__request_queue.get(block, timeout)

    def get_buffer_request(self, block=True, timeout=None):
        return self.__request_buffer_queue.get(block, timeout)

    def is_request_buffer_queue_empty(self):
        return self.__request_buffer_queue.empty()

    def request_done(self, request: Request):
        fingerprint = request.fingerprint
        self.__request_reply_queue.put((const.REPLY_REQUEST_DONE, fingerprint))
        self.__request_queue.task_done()

    def request_failed(self, request: Request):
        self.__request_reply_queue.put((const.REPLY_REQUEST_FAILED, request))
        self.__request_queue.task_done()

    # html response
    def add_parse_data(self, response: requests.models.Response, request: Request):
        self.__request_reply_queue.put((const.REPLY_REQUEST_PARSE, request))
        self.__parse_data_queue.put((response, request))

    def get_parse_data(self):
        return self.__parse_data_queue.get()

    def parse_done(self, request: Request):
        self.__request_reply_queue.put((const.REPLY_PARSE_DONE, request.fingerprint))

    def parse_failed(self, request: Request):
        self.__request_reply_queue.put((const.REPLY_PARSE_FAILED, request))

    def is_all_task_queue_empty(self):
        return (self.__request_queue.empty() and self.__parse_data_queue.empty() and self.__request_buffer_queue.empty())
