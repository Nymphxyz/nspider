#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: singleton.py
# @time: 2021.01.25 21:55
# @desc:

import threading
from abc import ABCMeta, abstractmethod


class Singleton(metaclass=ABCMeta):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, *args, **kwargs):
        if (self._initialized): return
        self._init_once(*args, **kwargs)
        self._initialized = True

    def _init_once(self):
        pass


class MultiThreadSingleton(metaclass=ABCMeta):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with cls._instance_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, *args, **kwargs):
        if (self._initialized): return
        self._init_once(*args, **kwargs)
        self._initialized = True

    def _init_once(self):
        pass