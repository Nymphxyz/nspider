#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: tps_bucket.py
# @time: 2021.02.24 20:36
# @desc:

import time
import threading
from multiprocessing import Value


class TPSBucket:
    def __init__(self, expected_tps):
        self.number_of_tokens = Value('i', 0)
        self.expected_tps = expected_tps
        self.bucket_refresh_thread = threading.Thread(target=self.refill_bucket_per_second)
        self.bucket_refresh_thread.setDaemon(True)

    def refill_bucket_per_second(self):
        while True:
            self.refill_bucket()
            time.sleep(1)

    def refill_bucket(self):
        self.number_of_tokens.value = self.expected_tps

    def start(self):
        self.bucket_refresh_thread.start()

    def stop(self):
        self.bucket_refresh_thread.kill()

    def get_token(self):
        response = False
        if self.number_of_tokens.value > 0:
            with self.number_of_tokens.get_lock():
                if self.number_of_tokens.value > 0:
                    self.number_of_tokens.value -= 1
                    response = True

        return response
