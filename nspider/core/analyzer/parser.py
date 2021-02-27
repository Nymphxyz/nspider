#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: parser.py
# @time: 2020.11.09 17:28
# @desc:

import os
import pickle
import sqlite3
import requests
from abc import ABCMeta, abstractmethod

from nspider.core.resource import Resource
import nspider.utilities.constant as const
from nspider.abstract.singleton import MultiThreadSingleton


class Parser(MultiThreadSingleton, metaclass=ABCMeta):

    def _init_once(self):
        self.db_path = os.path.join(const.CACHE_DIR, self.name() + ".db")
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.c = self.conn.cursor()
        self.c.execute('pragma journal_mode=wal;')
        create_data_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} BLOB)".format(
            const.PARSER_DATA_TABLE, const.COLUMN_NAME_DATA)
        create_resource_table = "CREATE TABLE if not exists {} (id INTEGER PRIMARY KEY, {} TEXT, {} BLOB)".format(
            const.PARSER_RESOURCE_TABLE, const.COLUMN_NAME_FINGERPRINT, const.COLUMN_NAME_RESOURCE)

        for t in [create_data_table, create_resource_table]:
            self.c.execute(t)

    def process(self, request, response: requests.models.Response, analyzer_handler):
        self.analyzer_handler = analyzer_handler
        self.logger = analyzer_handler.logger
        try:
            obj = self.parse(request, response)
            self.pipeline(obj)
            analyzer_handler.shared_memory_handler.parse_done(request)
        except Exception as err:
            raise err

    @abstractmethod
    def parse(self, request, response: requests.models.Response) -> (object, object):
        raise NotImplementedError

    def pipeline(self, obj):
        pass

    @classmethod
    def name(cls):
        return cls.__name__

    def request(self, *args, **kwargs):
        self.analyzer_handler.request(*args, **kwargs)

    def requests(self, *args, **kwargs):
        self.analyzer_handler.requests(*args, **kwargs)

    def parse_failed(self, *args, **kwargs):
        self.analyzer_handler.parse_failed(*args, **kwargs)

    def save(self, data: object):
        c = self.conn.cursor()
        c.execute("INSERT OR IGNORE INTO {}({}) VALUES (?)".format(const.PARSER_DATA_TABLE,
                                                                          const.COLUMN_NAME_DATA),
                  (pickle.dumps(data), ))
    def resource(self, url, filename=None, save_dir=None, *args, dupe_filter=False, **kwargs):
        new_resource = Resource(url, filename, save_dir, *args, **kwargs)

        c = self.conn.cursor()

        if dupe_filter:
            c.execute("SELECT {} FROM {} WHERE fingerprint=?".format(const.COLUMN_NAME_FINGERPRINT, const.PARSER_RESOURCE_TABLE), (new_resource.fingerprint,))
            if c.fetchone():
                self.logger.info("Refuse adding resource: {}".format(url))
                return False
        c.execute("INSERT OR IGNORE INTO {}({}, {}) VALUES (?, ?)".format(const.PARSER_RESOURCE_TABLE, const.COLUMN_NAME_FINGERPRINT,
                                                                   const.COLUMN_NAME_RESOURCE),
                  (new_resource.fingerprint, pickle.dumps(new_resource),))
        self.logger.info("Added resource: {}".format(url))
        return True



