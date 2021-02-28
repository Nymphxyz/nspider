#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: process_executor.py
# @time: 2021.01.28 13:12
# @desc:

import time
import logging
import threading
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, JoinableQueue

import nspider.utilities.constant as const
from nspider.core.log import MultiprocessLog

class ProcessExecutor(Process, metaclass=ABCMeta):

    def __init__(self,
                 name,
                 shared_memory_handler,
                 CORE_POOL_SIZE: int,
                 MAX_POOL_SIZE: int,
                 KEEP_ALIVE_TIME: float,
                 RETYR_NUM: int,
                 worker_class,
                 TPS=-1):
        super().__init__()
        self.__stop_signal = False
        self.__pause_signal = False

        self.name = name

        self.shared_memory_handler = shared_memory_handler

        self.TPS = TPS

        self.RETRY_NUM = RETYR_NUM

        self.CORE_POOL_SIZE = CORE_POOL_SIZE
        self.MAX_POOL_SIZE = MAX_POOL_SIZE

        self.KEEP_ALIVE_TIME = KEEP_ALIVE_TIME

        self.job_queue = JoinableQueue(self.MAX_POOL_SIZE * 2)
        self.worker_class = worker_class

        self.workers = {}
        self.worker_states = {}
        self.__worker_count = 0
        self.__init_worker_states()

    def __init_worker_states(self):
        for i in range(self.MAX_POOL_SIZE):
            self.worker_states[str(i)] = const.WORKER_EMPTY

    def __get_first_empty_worker_id(self) -> int:
        for i in range(self.MAX_POOL_SIZE):
            if self.worker_states[str(i)] == const.WORKER_EMPTY:
                return i

    @property
    def pause_signal(self):
        return self.__pause_signal

    @property
    def stop_signal(self):
        return self.__stop_signal

    @property
    def worker_count(self):
        count = 0
        for k, v in self.worker_states.items():
            if v != const.WORKER_EMPTY:
                count += 1
        return count

    @stop_signal.setter
    def stop_signal(self, signal: bool):
        self.__stop_signal = signal

    @pause_signal.setter
    def pause_signal(self, signal: bool):
        self.__pause_signal = signal

    @abstractmethod
    def get_job(self) -> object:
        raise NotImplementedError

    @abstractmethod
    def create_worker(self, worker_class, is_core=True, init_job=None) -> object:
        raise NotImplementedError

    def update_worker_state(self, id_, state):
        if not self.worker_states.get(id_):
            return
        else:
            self.worker_states[id_] = state
            if state == const.WORKER_EMPTY:
                self.workers[id_] = None

    def __receiver_process(self):
        while not self.stop_signal:
            if self.pause_signal:
                time.sleep(1)
                continue

            job = self.get_job()
            # self.logger.info(self.worker_states)
            if self.worker_count < self.CORE_POOL_SIZE:
                core_worker = self.create_worker(self.worker_class, str(self.__get_first_empty_worker_id()), is_core=True, init_job=job)
                self.workers[str(core_worker.id)] = core_worker
                self.update_worker_state(core_worker.id, const.WORKER_INIT)
                core_worker.start()
            elif self.job_queue.full():
                if self.worker_count < self.MAX_POOL_SIZE:
                    non_core_worker = self.create_worker(self.worker_class, str(self.__get_first_empty_worker_id()), is_core=False, init_job=job)
                    self.workers[str(non_core_worker.id)]= non_core_worker
                    self.update_worker_state(non_core_worker.id, const.WORKER_INIT)
                    non_core_worker.start()
                else:
                    self.job_queue.put(job)
            else:
                self.job_queue.put(job)

    def __init_inner_log(self):
        MultiprocessLog.worker_configurer(self.shared_memory_handler.log_message_queue)
        self.logger = logging.getLogger(self.name)

    def ran(self):
        pass

    def __init_receiver(self):
        self.receiver = threading.Thread(target=self.__receiver_process)
        self.receiver.setDaemon(True)
        self.receiver.start()

    def before_waiting(self):
        pass

    def __wait_for_thread(self):
        self.receiver.join()

    def run(self):
        self.__init_inner_log()
        self.ran()
        self.__init_receiver()
        self.before_waiting()
        self.__wait_for_thread()





