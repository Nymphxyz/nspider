#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: fetcher_worker.py
# @time: 2020.11.25 12:03 PM
# @desc:

import time
import logging

from nspider.utilities.common import *
from nspider.abstract.process_executor_worker import ProcessExecutorWorker


class FetcherWorker(ProcessExecutorWorker):

    def ran(self):
        self.logger = logging.getLogger('fetcher_worker')
        self.logger.info("Fetcher Worker {}: start to work.".format(self.id))

    def apply_init_job(self):
        self.apply_request(self.init_job)

    def apply_job(self, job):
        self.apply_request(job)

    def apply_request(self, request):
        #   time.sleep(0.01)
        for i in range(self.RETRY_NUM):
            while True:
                if self.process_handler.tps_bucket.get_token():
                    break
            self.logger.info("Fetcher worker {} trying accessing url [try:{}]: {}".format(self.id, i, request.url))
            response, err = get_res_from_request(request)
            if response:
                break

        if response:
            self.logger.debug("Fetcher worker {} get response".format(self.id))
            if request.callback:
                request.callback(response)
            if request.parser_class:
                self.shared_memory_handler.add_parse_data(response, request)
            self.shared_memory_handler.request_done(request)
        else:
            self.logger.warning("Fetcher worker {} failed access url: {} Reason: {}".format(self.id, request.url, err))
            if request.errback:
                request.errback(err)
            self.shared_memory_handler.request_failed(request)

    def stop(self):
        self.stop_signal = True
        self.logger.warning("Fetcher Worker {}: is set to stop.".format(self.id))

    def pause(self):
        self.pause_signal = False
        self.logger.warning("Fetcher Worker {}: is set to pause.".format(self.id))

    def resume(self):
        if self.pause_signal:
            self.pause_signal = False
        else:
            self.logger.warning("Fetcher Worker {}: No need to resume.".format(self.id))
