#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: process_executor_worker.py
# @time: 2021.01.28 15:21
# @desc:

import time
import logging
import threading
import queue
from abc import ABCMeta, abstractmethod

import nspider.utilities.constant as const


class ProcessExecutorWorker(threading.Thread, metaclass=ABCMeta):

    def __init__(self, id_: str, name:str, process_handler, shared_memory_handler, RETRY_NUM: int, job_queue, KEEP_ALIVE_TIME=-1, init_job=None):
        super().__init__()
        self.__id = id_    # continue working or not
        self.name = name
        self.__stop_signal = False
        self.__pause_signal = False

        self.process_handler = process_handler

        self.keep_alive_timer = 0
        self.KEEP_ALIVE_TIME = KEEP_ALIVE_TIME

        self.RETRY_NUM = RETRY_NUM
        self.job_queue = job_queue
        self.init_job = init_job
        self.shared_memory_handler = shared_memory_handler
        self.setDaemon(True)


    @property
    def id(self):
        return self.__id

    @property
    def pause_signal(self):
        return self.__pause_signal

    @property
    def stop_signal(self):
        return self.__stop_signal

    @stop_signal.setter
    def stop_signal(self, signal: bool):
        self.__stop_signal = signal
        self.logger.debug("{} {}: set stop flag to {}.".format(self.name, self.__id, signal))

    @pause_signal.setter
    def pause_signal(self, signal: bool):
        self.__pause_signal = signal
        self.logger.debug("{} {}: set pause flag to {}.".format(self.name, self.__id, signal))

    @abstractmethod
    def ran(self):
        raise NotImplementedError

    def __init_inner_log(self):
        self.logger = logging.getLogger(self.name)

    @abstractmethod
    def apply_init_job(self):
        raise NotImplementedError

    @abstractmethod
    def apply_job(self, job):
        raise NotImplementedError

    def run(self):
        self.process_handler.update_worker_state(self.id, const.WORKER_START)
        self.__init_inner_log()
        self.ran()

        try:
            if self.init_job:
                self.apply_init_job()
                self.init_job = None

            while not self.stop_signal:
                if self.pause_signal:
                    time.sleep(1)
                    continue
                self.timer = 0
                if self.KEEP_ALIVE_TIME > 0:
                    try:
                        job = self.job_queue.get(timeout=self.KEEP_ALIVE_TIME)
                    except queue.Empty:
                        self.stop_signal = True
                        self.logger.warning("Time out {} is going to shut down after completing it's work".format(self.name))
                        continue
                else:
                    job = self.job_queue.get()

                self.logger.debug("Get job from inner queue: {}".format(time.time()))
                self.apply_job(job)
                self.job_queue.task_done()

            if self.stop_signal:
                self.process_handler.update_worker_state(self.id, const.WORKER_EMPTY)

            self.logger.info("{} closed".format(self.name))
        except Exception as err:
            self.logger.exception(err)
            self.process_handler.update_worker_state(self.id, const.WORKER_EMPTY)
