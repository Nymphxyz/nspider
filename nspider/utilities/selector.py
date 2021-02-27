#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @license: (C) Copyright 2020-2020
# @contact: xyz.hack666@gmail.com
# @file: selector.py
# @time: 2020.11.25 5:08 PM
# @desc:

from lxml import etree
from bs4 import BeautifulSoup


class Selector(object):

    def __init__(self, content):
        # xpath
        self.xpath_root = etree.HTML(content)
        # BeautifulSoup
        self.soup = BeautifulSoup(content, 'lxml')

    def xpath(self, path: str) -> object:
        return self.xpath_root.xpath(path)



