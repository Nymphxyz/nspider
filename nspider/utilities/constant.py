#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: constant.py
# @time: 2021.02.11 16:03
# @desc:

CACHE_DIR = ".cache"

WORKER_EMPTY = "WORKER_EMPTY"
WORKER_INIT = "WORKER_INIT"
WORKER_START = "WORKER_START"
WORKER_PAUSE = "WORKER_PAUSE"
WORKER_STOP = "WORKER_STOP"

REPLY_REQUEST_FAILED = "a"
REPLY_REQUEST_DONE = "b"
REPLY_REQUEST_PARSE = "c"
REPLY_PARSE_FAILED = "d"
REPLY_PARSE_DONE = "e"

REQUEST_BUFFER_TABLE = "request_buffer"
REQUEST_DONE_TABLE = "request_done"
REQUEST_FAILED_TABLE = "request_failed"
REQUEST_IN_PROCESS_TABLE = "request_in_process"
REQUEST_PARSE_TABLE = "request_parse"
REQUEST_PARSE_FAILED_TABLE = "request_parse_failed"
RESOURCE_DONE_TABLE = "resource_done"
RESOURCE_FAILED_TABLE = "resource_failed"

PARSER_DATA_TABLE = "parser_data"
PARSER_RESOURCE_TABLE = "parser_resource"

COLUMN_NAME_URL = "url"
COLUMN_NAME_PARAMS = "params"
COLUMN_NAME_REQUEST = "request"
COLUMN_NAME_FINGERPRINT = "fingerprint"
COLUMN_NAME_RESOURCE = "resource"

COLUMN_NAME_DATA = "data"

