#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: analyzer_worker.py
# @time: 2021.01.25 18:02
# @desc:

from nspider.abstract.process_executor_worker import ProcessExecutorWorker

class AnalyzerWorker(ProcessExecutorWorker):
    def ran(self):
        self.logger.info("Analyzer Worker {}: start to work.".format(self.id))

    def apply_init_job(self):
        self.parse_data(self.init_job)

    def apply_job(self, job):
        self.parse_data(job)

    def parse_data(self, job):
        self.logger.info("Analyzer worker {} trying parsing data".format(self.id))
        try:
            (response, request) = job
            if request.parser_class:
                request.parser_class().process(request, response, self.process_handler)

        except Exception as err:
            self.logger.exception(err)

    def stop(self):
        self.__stop_signal = True
        self.logger.warning("Analyzer Worker {}: is set to stop.".format(self.__id))

    def pause(self):
        self.__pause_signal = False
        self.logger.warning("Analyzer Worker {}: is set to pause.".format(self.__id))

    def resume(self):
        if self.__pause_signal:
            self.__pause_signal = False
        else:
            self.logger.warning("Analyzer Worker {}: No need to resume.".format(self.__id))

