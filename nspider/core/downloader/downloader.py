#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: downloader.py
# @time: 2021.02.23 16:27
# @desc:

import os
import pickle
import sqlite3
import threading
from  multiprocessing import JoinableQueue

from nspider.core.log import Log
import nspider.utilities.constant as const
from nspider.core.tps_bucket import TPSBucket
import nspider.utilities.common as common


class Downloader(object):

    def __init__(self, tps, thread_num):
        self.tps = tps
        self.thread_num = thread_num
        self.tps_bucket = TPSBucket(expected_tps=self.tps)
        self.tps_bucket.start()

        self.resource_queue = JoinableQueue(self.thread_num*2)

        self.workers = []

    def start(self, parser, clear_cache=False):
        self.log = Log(parser.name() + ".download.log")
        self.parser_db_path = os.path.join(const.CACHE_DIR, parser.name() + ".db")
        self.db_path =  os.path.join(const.CACHE_DIR, parser.name() + ".download.db")

        if clear_cache:
            try:
                os.remove(self.db_path)
            except Exception as err:
                self.log.logger.exception(err)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.conn_p = sqlite3.connect(self.parser_db_path, check_same_thread=False, isolation_level=None)

        self.__init_db()
        self.__init_fetcher()
        self.__init_workers()

        self.__wait()
        self.log.logger.info("Exit")

    def __init_workers(self):
        for i in range(self.thread_num):
            worker = threading.Thread(target=self.__worker_process, args=(str(i),))
            worker.setDaemon(True)
            self.workers.append(worker)
        for worker in self.workers:
            worker.start()

    def __init_fetcher(self):
        self.fetcher = threading.Thread(target=self.__fetcher_process)
        self.fetcher.setDaemon(True)
        self.fetcher.start()

    def __wait(self):
        self.fetcher.join()

    def __init_db(self):
        c = self.conn.cursor()
        c.execute('pragma journal_mode=wal;')

        create_resource_done_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} TEXT)".format(
            const.RESOURCE_DONE_TABLE, const.COLUMN_NAME_FINGERPRINT)
        create_resource_failed_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} TEXT, {} BLOB)".format(
            const.RESOURCE_FAILED_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_RESOURCE)

        for t in [create_resource_done_table, create_resource_failed_table]:
            c.execute(t)

    def __fetcher_process(self):
        c= self.conn.cursor()
        c_p = self.conn_p.cursor()
        resources = c_p.execute("SELECT * FROM {}".format(const.PARSER_RESOURCE_TABLE))

        for resource in resources:
            c.execute("SELECT {} FROM {} WHERE id=? AND fingerprint=?".format(const.COLUMN_NAME_FINGERPRINT,
                                                                     const.RESOURCE_DONE_TABLE),
                      (resource[0], resource[1],))
            if c.fetchone():
                pass
            else:
                self.resource_queue.put((resource[0], pickle.loads(resource[2])))

        self.log.logger.info("Fetcher put all resources in the queue, now waiting for workers finish jobs")
        self.resource_queue.join()

    def __worker_process(self, name):
        self.log.logger.info("Worker {} start to work".format(name))
        c = self.conn.cursor()
        while True:
            (id_, resource) = self.resource_queue.get()
            while True:
                if self.tps_bucket.get_token():
                    break

            self.log.logger.info("Worker {} trying downloading {}".format(name, resource.url))
            res = self.download(resource)
            if res:
                c.execute("INSERT OR IGNORE INTO {}(id, {}) VALUES (?, ?)".format(const.RESOURCE_DONE_TABLE,
                                                                                  const.COLUMN_NAME_FINGERPRINT),
                         (id_, resource.fingerprint,))
                if (resource.callback):
                    resource.callback()
            else:
                c.execute("INSERT OR IGNORE INTO {}(id, {}, {}) VALUES (?, ?, ?)".format(const.RESOURCE_FAILED_TABLE,
                                                                                  const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_RESOURCE),
                          (id_, resource.fingerprint, pickle.dumps(resource)))
                if(resource.errback):
                    resource.errback()
            self.resource_queue.task_done()

    def download(self, resource):
        return common.download(resource.url, filename=resource.filename, save_dir=resource.save_dir, stream=resource.stream,
                               verbose=resource.verbose, try_num=resource.try_num, fix_type=resource.fix_type, cookies=resource.cookies,
                      headers=resource.headers, params=resource.params, data=resource.data, session=resource.session, log=self.log.logger.info,
                               log_exception=self.log.logger.exception)
