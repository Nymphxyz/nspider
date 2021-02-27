#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: spider.py
# @time: 2020.11.09 17:21
# @desc:

import requests

from .log import *
from nspider.settings import Settings
import nspider.utilities.constant as const
from nspider.core.fetcher.fetcher import Fetcher
from nspider.core.analyzer.analyzer import Analyzer
from nspider.core.scheduler.scheduler import Scheduler
from .shared_memory_handler import SharedMemoryHandler
from nspider.core.fetcher.fetcher_worker import FetcherWorker

# TODO: set session kookie set start request's attributes
class Spider(object):
    start_url = []
    start_parser = []
    start_proxies = {}
    start_session = {}
    start_COOKIES = {}
    start_headers = Settings.HEADERS


    def __init__(self, settings=Settings(), use_cache=True):
        assert isinstance(settings, Settings), "Variable settings is not a Settings type: %r" % settings

        settings.DB = self.name() + ".db"
        settings.BUFFER_DB = self.name() + ".buffer.db"

        settings.auto_correct()
        settings.check_validity()

        self.settings = settings

        if not os.path.exists(const.CACHE_DIR):
            os.makedirs(const.CACHE_DIR)

        self.shared_memory_handler = SharedMemoryHandler(self.settings)

        MultiprocessLog.worker_configurer(self.shared_memory_handler.log_message_queue)
        self.log = MultiprocessLog()
        self.log.logger.info("Init multiprocess log listener...")
        self.log.start_listener(self.shared_memory_handler.log_message_queue)

        self.log.logger.info("Init scheduler...")
        self.scheduler = Scheduler(self.settings, self.shared_memory_handler, use_cache=use_cache)

        self.log.logger.info("Init fetcher...")
        self.fetcher = Fetcher("fetcher",
                               self.shared_memory_handler,
                               self.settings.CORE_FETCHER_WORKER_POOL_SIZE,
                               self.settings.MAX_FETCHER_WORKER_POOL_SIZE,
                               self.settings.KEEP_ALIVE_TIME_FOR_NON_CORE_THREAD,
                               self.settings.RETRY_NUM,
                               FetcherWorker,
                               TPS=self.settings.TPS)

        self.log.logger.info("Init analyzer...")
        self.analyzer = Analyzer("analyzer",
                               self.shared_memory_handler,
                               self.settings.CORE_FETCHER_WORKER_POOL_SIZE,
                               self.settings.MAX_FETCHER_WORKER_POOL_SIZE,
                               self.settings.KEEP_ALIVE_TIME_FOR_NON_CORE_THREAD,
                               self.settings.RETRY_NUM,
                               FetcherWorker)

    def start(self):
        self.scheduler.daemon = True
        self.fetcher.daemon = True
        self.analyzer.daemon = True

        self.scheduler.start()
        self.fetcher.start()
        self.analyzer.start()

        try:
            self.requests(self.start_url, parser_classes=self.start_parser, cookies=self.start_COOKIES, session=self.start_session, proxies=self.start_proxies, headers=self.start_headers)
        except Exception as err:
            self.scheduler.kill()
            self.fetcher.kill()
            self.analyzer.kill()
            self.log.logger.exception(err)
        else:
            self.scheduler.join()
            self.fetcher.join()
            self.analyzer.join()

    def request(self, url: str, parser_class=None, cookies=None, session=None, proxies=None, headers=None):
        if not url:
            raise Exception("No URL specified!")

        if not session:
            session = requests.session()

        self.shared_memory_handler.add_request_in_buffer(url, parser_class if parser_class else None, cookies=cookies, session=session, proxies=proxies, headers=headers)
        self.log.logger.debug("Added request: {}".format(url))

    def requests(self, urls: list, parser_classes=None, cookies=None, session=None, proxies=None, headers=None):
        if not urls:
            raise Exception("No URLs specified!")

        if parser_classes:
            if len(parser_classes) > 1:
                if len(parser_classes) != len(urls):
                    raise Exception("Can‘t match parse with URL")
                else:
                    for index, url in enumerate(urls):
                        self.request(url, parser_class=parser_classes[index], cookies=cookies, session=session, proxies=proxies, headers=headers)
            else:
                for url in urls:
                    self.request(url, parser_class=parser_classes[0], cookies=cookies, session=session, proxies=proxies, headers=headers)

    @classmethod
    def name(cls):
        return cls.__name__
