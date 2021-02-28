#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @author: XYZ
# @file: setup.py
# @time: 2021.02.27 15:14
# @desc:

from setuptools import setup, find_packages

def readme_file():
    with open("README.rst") as f:
        return f.read()

setup(name="nspider",
      version="0.2.0",
      description="A light multi-process web resources crawling framework",
      long_description=readme_file(),
      license="MIT",
      packages=find_packages(exclude=["demo", "img"]),
      author="Nymphxyz",
      author_email="xyz.hack666@gmail.com",
      url="https://github.com/Nymphxyz/nspider",
      install_requires=["requests", "lxml", "beautifulsoup4"])

