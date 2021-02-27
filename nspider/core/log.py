#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: logger.py
# @time: 2021.01.25 13:09
# @desc:
# @references:
# https://www.cnblogs.com/chongyou/p/9370201.html
# https://fanchenbao.medium.com/python3-logging-with-multiprocessing-f51f460b8778

import os
import time
import logging
import logging.handlers

from nspider.abstract.singleton import Singleton
import multiprocessing


class MultiprocessLog(object):

    def __init__(self):
        self.logger = logging.getLogger('main')
        self.logs_dir = "logs"


    def start_listener(self, queue):
        self.process = multiprocessing.Process(target=MultiprocessLog.listener_process, args=(queue,))
        self.process.start()

    @staticmethod
    def __listener_configurer():
        # 创建文件目录
        logs_dir = "logs"
        if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
            pass
        else:
            os.mkdir(logs_dir)

        # 修改log保存位置
        timestamp = time.strftime("%Y-%m-%d", time.localtime())
        log_file_name = '%s.log' % timestamp
        log_file_path = os.path.join(logs_dir, log_file_name)

        root = logging.getLogger()
        root.setLevel(logging.INFO)
        # 设置输出格式
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)-8s] [%(filename)-30s %(funcName)-30s (line:%(lineno)03d)] %(message)s',
            '%Y-%m-%d %H:%M:%S')

        rotating_file_handler = logging.handlers.RotatingFileHandler(filename=log_file_path,
                                                                     mode='w',
                                                                     maxBytes=1024 * 1024 * 50,
                                                                     backupCount=0,
                                                                     encoding='utf-8')
        rotating_file_handler.setFormatter(formatter)
        rotating_file_handler.setLevel(logging.INFO)
        # 控制台句柄
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        # 添加内容到日志句柄中
        root.addHandler(rotating_file_handler)
        root.addHandler(console_handler)

    @staticmethod
    def worker_configurer(queue):
        h = logging.handlers.QueueHandler(queue)
        root = logging.getLogger()
        root.addHandler(h)
        root.setLevel(logging.INFO)

    @staticmethod
    def listener_process(queue):
        MultiprocessLog.__listener_configurer()
        while True:
            while not queue.empty():
                record = queue.get()
                logger = logging.getLogger(record.name)
                logger.handle(record)  # No level or filter logic applied - just do it!
            time.sleep(1)


class Log(Singleton):

    def _init_once(self, filename):
        self.logger = logging.getLogger("main")

        # 创建文件目录
        logs_dir = "logs"
        if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
            pass
        else:
            os.mkdir(logs_dir)

        # 修改log保存位置
        log_file_name = filename
        log_file_path = os.path.join(logs_dir, log_file_name)
        # 设置输出格式
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)-8s] [%(filename)-30s %(funcName)-30s (line:%(lineno)03d)] %(message)s',
            '%Y-%m-%d %H:%M:%S')
        rotating_file_handler = logging.handlers.RotatingFileHandler(filename=log_file_path,
                                                                   mode='a',
                                                                   maxBytes=1024 * 1024 * 50,
                                                                   backupCount=0)

        rotating_file_handler.setFormatter(formatter)
        rotating_file_handler.setLevel(logging.INFO)

        # 控制台句柄
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.NOTSET)
        console_handler.setFormatter(formatter)

        # 添加内容到日志句柄中
        self.logger.addHandler(rotating_file_handler)
        self.logger.addHandler(console_handler)
        self.logger.setLevel(logging.INFO)
