#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: scheduler.py
# @time: 2020.11.19 16:46
# @desc:
# @references:
# https://charlesleifer.com/blog/going-fast-with-sqlite-and-python/

import os
import time
import queue
import pickle
import sqlite3
import logging
import threading
from multiprocessing import Queue, Process

import nspider.utilities.constant as const
from nspider.core.log import MultiprocessLog
from nspider.settings import Settings
from nspider.core.request import Request


class Scheduler(Process):

    def __init__(self, settings: Settings, shared_memory_handler, use_cache=True):
        super().__init__()
        
        self.stop_signal = False
        self.pause_signal = False

        self.fusing_flag = False

        self.use_cache = use_cache

        self.settings = settings
        self.shared_memory_handler = shared_memory_handler
        # inner request queue
        self.__inner_request_queue = Queue()

        # request 是否正在处理
        self.__request_in_process_fingerprint = set()
        # 已经成功完成的 request 的 fingerprint
        self.__request_done_fingerprint = set()
        # 失败的请求
        self.__request_failed = set()

        self.db_path = os.path.join(const.CACHE_DIR, self.settings.DB)

        self.__init_db()

    def __init_db(self):
        if not os.path.exists(self.db_path):
            with open(self.db_path, "wb") as f:
                pass
        elif not os.path.isfile(self.db_path):
            raise Exception("Naming conflicts: Can't create db with this name: ".format(self.db_path))

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('pragma journal_mode=wal;')

        create_request_done_table = "CREATE TABLE if not exists {} ({} TEXT PRIMARY KEY)".format(const.REQUEST_DONE_TABLE, const.COLUMN_NAME_FINGERPRINT)
        create_request_buffer_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} BLOB)".format(const.REQUEST_BUFFER_TABLE, const.COLUMN_NAME_PARAMS)
        create_request_failed_table = "CREATE TABLE if not exists {} ({} TEXT PRIMARY KEY, {} BLOB)".format(const.REQUEST_FAILED_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST)
        create_request_in_process_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} TEXT, {} BLOB)".format(const.REQUEST_IN_PROCESS_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST)
        create_request_parse_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} TEXT, {} BLOB)".format(const.REQUEST_PARSE_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST)
        create_request_parse_failed_table = "CREATE TABLE if not exists {} ({} TEXT PRIMARY KEY, {} BLOB)".format(const.REQUEST_PARSE_FAILED_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST)

        tables = [create_request_in_process_table, create_request_buffer_table, create_request_done_table, create_request_failed_table, create_request_parse_table, create_request_parse_failed_table]

        for t in tables:
            c.execute(t)

        conn.close()

    def __load_cache(self):
        c = self.conn.cursor()

        in_process_requests = c.execute("SELECT * FROM {}".format(const.REQUEST_IN_PROCESS_TABLE))
        for r in in_process_requests:
            new_request = pickle.loads(r[2])
            self.__inner_request_queue.put(new_request)
            self.__request_in_process_fingerprint.add(r[1])
            self.logger.info("Re-added request which still in processing: {}".format(new_request.url))

        requests_parse = c.execute("SELECT * FROM {}".format(const.REQUEST_PARSE_TABLE))
        for r in requests_parse:
            new_request = pickle.loads(r[2])
            self.__add_request(self.c, new_request=new_request)
            # pretend parse is done, delete this data now. It will add to request_parse later anyway.
            self.parse_done(self.c, r[1])
            self.logger.info("Re-added request which still in parsing or failed to parse: {}".format(new_request.url))


        requests_done = c.execute("SELECT * FROM {}".format(const.REQUEST_DONE_TABLE))
        for r in requests_done:
            self.__request_done_fingerprint.add(r[0])

        requests_failed = c.execute("SELECT * FROM {}".format(const.REQUEST_FAILED_TABLE))
        for r in requests_failed:
            self.__request_failed.add(pickle.loads(r[1]))

    def run(self):
        MultiprocessLog.worker_configurer(self.shared_memory_handler.log_message_queue)
        self.logger = logging.getLogger(self.name)

        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.c = self.conn.cursor()

        if self.use_cache:
            self.__load_cache()

        self.__init_reply_receiver()
        self.__init_request_manager()
        self.__init_request_transfer()

        self.reply_receiver.join()
        self.request_manager.join()
        self.request_transfer.join()

    def __init_reply_receiver(self):
        self.reply_receiver = threading.Thread(target=self.__reply_receiver_process)
        self.reply_receiver.setDaemon(True)
        self.reply_receiver.start()

    def __reply_receiver_process(self):
        c = self.conn.cursor()

        while not self.stop_signal:
            if self.pause_signal:
                time.sleep(1)
                continue

            (reply_type, data) = self.shared_memory_handler.get_request_reply()
            if reply_type == const.REPLY_REQUEST_DONE:
                self.request_done(c, data)
            elif reply_type == const.REPLY_REQUEST_FAILED:
                self.request_failed(c, data)
            elif reply_type == const.REPLY_REQUEST_PARSE:
                self.request_parse(c, data)
            elif reply_type == const.REPLY_PARSE_DONE:
                self.parse_done(c, data)
            elif reply_type == const.REPLY_PARSE_FAILED:
                self.parse_failed(c, data)

    def __init_request_transfer(self):
        self.request_transfer = threading.Thread(target=self.__request_transfer_process)
        self.request_transfer.setDaemon(True)
        self.request_transfer.start()

    def __request_transfer_process(self):
        while not self.stop_signal:
            if self.pause_signal:
                time.sleep(1)
                continue

            request = self.get_inner_request()
            self.shared_memory_handler.add_request(new_request=request)

    def __init_request_manager(self):
        self.request_manager = threading.Thread(target=self.__request_manager_process)
        self.request_manager.setDaemon(True)
        self.request_manager.start()

    def __request_manager_process(self):
        c = self.conn.cursor()

        while not self.stop_signal:
            if self.pause_signal:
                time.sleep(1)
                continue

            try:
                (args, kwargs) = self.shared_memory_handler.get_buffer_request(timeout=1)
            except queue.Empty:
                if self.shared_memory_handler.is_all_task_queue_empty():
                    res = c.execute("SELECT * FROM {} LIMIT ?".format(const.REQUEST_BUFFER_TABLE), (self.settings.BUFFER_REQUEST_THRESHOLD, ))
                    if res:
                        for row in res:
                            id_ = row[0]
                            (args, kwargs) = pickle.loads(row[1])
                            self.__add_request(c, *args, **kwargs)
                            c.execute("DELETE OR IGNORE FROM {} WHERE id=?".format(const.REQUEST_BUFFER_TABLE), (id_, ))
                    else:
                        self.fusing_flag = False
            else:
                if self.__inner_request_queue.qsize() > self.settings.BUFFER_REQUEST_THRESHOLD:
                    self.fusing_flag = True

                if self.fusing_flag:
                    self.__add_request_in_buffer(c, (args, kwargs))
                else:
                    res = self.__add_request(c, *args, **kwargs)
                    if not res:
                        self.logger.warning("Refuse adding request: {}".format(args[0]))
                    else:
                        self.logger.info("Added request: {}".format(args[0]))
                        self.logger.debug("Added request in buffer queue: {}".format(time.time()))

    def __add_request_in_buffer(self, c, params):
        c.execute("INSERT INTO {}({}) VALUES (?,)".format(const.REQUEST_BUFFER_TABLE, const.COLUMN_NAME_PARAMS), (pickle.dumps(params),))

    @property
    def request_done_fingerprint(self):
        return self.__request_done_fingerprint

    def __add_request(self, c, *args, new_request=None, in_process_filter=True, dupe_filter=True, block=True, timeout=None, **kwargs):
        if not new_request:
            new_request = Request(*args, **kwargs)
        fingerprint = new_request.fingerprint

        if in_process_filter:
            if fingerprint in self.__request_in_process_fingerprint:
                return False

        if dupe_filter:
            if fingerprint in self.__request_done_fingerprint:
                return False

        self.__request_in_process_fingerprint.add(fingerprint)
        c.execute("INSERT OR IGNORE INTO {}({}, {}) VALUES (?, ?)".format(const.REQUEST_IN_PROCESS_TABLE,
                                                                          const.COLUMN_NAME_FINGERPRINT,
                                                                          const.COLUMN_NAME_REQUEST),
                  (fingerprint, pickle.dumps(new_request)))

        self.__inner_request_queue.put(new_request, block, timeout)
        return True

    def get_inner_request(self, block=True, timeout=None):
        return self.__inner_request_queue.get(block, timeout)

    def request_done(self, c, fingerprint):
        self.__request_done_fingerprint.add(fingerprint)
        self.__request_in_process_fingerprint.discard(fingerprint)

        c.execute("INSERT OR IGNORE INTO {}({}) VALUES (?)".format(const.REQUEST_DONE_TABLE, const.COLUMN_NAME_FINGERPRINT), (fingerprint, ))
        c.execute("DELETE FROM {} WHERE id IN (SELECT id FROM {} WHERE {}=? ORDER BY id LIMIT 1)".format(const.REQUEST_IN_PROCESS_TABLE, const.REQUEST_IN_PROCESS_TABLE, const.COLUMN_NAME_FINGERPRINT), (fingerprint, ))

    def request_failed(self, c, request: Request):
        self.__request_failed.add(request)
        c.execute("INSERT OR IGNORE INTO {}({}, {}) VALUES (?, ?)".format(const.REQUEST_FAILED_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST), (request.fingerprint, pickle.dumps(request), ))

    def request_parse(self, c, request: Request):
        c.execute("INSERT OR IGNORE INTO {}({}, {}) VALUES (?, ?)".format(const.REQUEST_PARSE_TABLE,
                                                                          const.COLUMN_NAME_FINGERPRINT,
                                                                          const.COLUMN_NAME_REQUEST),
                  (request.fingerprint, pickle.dumps(request),))

    def parse_done(self, c, fingerprint):
        c.execute("DELETE FROM {} WHERE id IN (SELECT id FROM {} WHERE {}=? ORDER BY id LIMIT 1)".format(const.REQUEST_PARSE_TABLE, const.REQUEST_PARSE_TABLE, const.COLUMN_NAME_FINGERPRINT),
                  (fingerprint,))

    def parse_failed(self, c, request: Request):
        c.execute("INSERT OR IGNORE INTO {}({}, {}) VALUES (?, ?)".format(const.REQUEST_PARSE_FAILED_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_REQUEST), (request.fingerprint, pickle.dumps(request),))
