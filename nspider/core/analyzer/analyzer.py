#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: analyzer.py
# @time: 2021.01.26 14:11
# @desc:

import time
from multiprocessing import JoinableQueue

from nspider.abstract.process_executor import ProcessExecutor


class Analyzer(ProcessExecutor):

    def __init__(self,
                 name,
                 shared_memory_handler,
                 CORE_POOL_SIZE: int,
                 MAX_POOL_SIZE: int,
                 KEEP_ALIVE_TIME: float,
                 RETYR_NUM: int,
                 worker_class):
        super().__init__(name,
                 shared_memory_handler,
                 CORE_POOL_SIZE,
                 MAX_POOL_SIZE,
                 KEEP_ALIVE_TIME,
                 RETYR_NUM,
                 worker_class)
        self.__request_buffer_queue = JoinableQueue(-1)
        self.fusing_flag = False

    def get_job(self):
        (response, request) = self.shared_memory_handler.get_parse_data()
        # init parser
        request.parser_class()
        self.logger.debug("get parse data from scheduler queue: {}".format(time.time()))
        return (response, request)

    def create_worker(self, worker_class, id_, is_core, init_job):
        if is_core:
            self.logger.info("Create core analyzer worker {}".format(id_))
            worker = worker_class(id_,
                                   "analyzer worker " + id_,
                                   self,
                                   self.shared_memory_handler,
                                   self.RETRY_NUM,
                                   self.job_queue,
                                   init_job=init_job)
        else:
            self.logger.info("Create none core analyzer worker {}".format(id_))
            worker = worker_class(id_,
                                   "analyzer worker " + id_,
                                   self,
                                   self.shared_memory_handler,
                                   self.RETRY_NUM,
                                   self.job_queue,
                                   KEEP_ALIVE_TIME=self.KEEP_ALIVE_TIME,
                                   init_job=init_job)
        return worker

    def request(self, url: str, *args, parser_class=None, **kwargs):
        if not url:
            self.logger.warning("No URL specified!")

        self.shared_memory_handler.add_request_in_buffer(url, parser_class if parser_class else None, *args, **kwargs)

        self.logger.debug("Added request in buffer queue: {}".format(time.time()))

    def requests(self, urls: list, *args, parser_classes=None, **kwargs):
        if not urls:
            raise Exception("No URLs specified!")

        if parser_classes:
            if len(parser_classes) > 1:
                if len(parser_classes) != len(urls):
                    self.logger.warning("Can‘t match parse with URL")
                else:
                    for index, url in enumerate(urls):
                        self.request(url, *args, parser_class=parser_classes[index], **kwargs)
            else:
                for url in urls:
                    self.request(url, parser_class=parser_classes[0])

    def parse_done(self, request):
        self.shared_memory_handler.parse_done(request)

    def parse_failed(self, request):
        self.shared_memory_handler.parse_failed(request)