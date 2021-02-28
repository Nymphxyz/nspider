#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: fetcher.py
# @time: 2020.11.24 4:19 PM
# @desc:
# @references:
# https://stackoverflow.com/questions/26098711/limiting-number-of-http-requests-per-second-on-python


import time

from nspider.core.tps_bucket import TPSBucket
from nspider.abstract.process_executor import ProcessExecutor


class Fetcher(ProcessExecutor):

    def get_job(self):
        request = self.shared_memory_handler.get_request()
        self.logger.debug("get request from scheduler queue: {}".format(time.time()))
        return request

    def ran(self):
        self.tps_bucket = TPSBucket(expected_tps=self.TPS)
        self.tps_bucket.start()

    def create_worker(self, worker_class, id_, is_core, init_job):
        if is_core:
            self.logger.info("Create core fetcher worker {}".format(id_))
            worker = worker_class(id_,
                                   "fetcher worker " + id_,
                                   self,
                                   self.shared_memory_handler,
                                   self.RETRY_NUM,
                                   self.job_queue,
                                   init_job=init_job)
        else:
            self.logger.info("Create none core fetcher worker {}".format(id_))
            worker = worker_class(id_,
                                   "fetcher worker " + id_,
                                   self,
                                   self.shared_memory_handler,
                                   self.RETRY_NUM,
                                   self.job_queue,
                                   KEEP_ALIVE_TIME=self.KEEP_ALIVE_TIME,
                                   init_job=init_job)
        return worker
