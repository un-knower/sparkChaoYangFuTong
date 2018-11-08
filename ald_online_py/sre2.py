#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 18-8-15 下午3:26
# @Author  : Shark
# @Site    : 
# @File    : sre2.py
# @Software: PyCharm

def number(argument):
    #gcs:这是定义一个字典映射。这个switche是可以自定义的，你可以取任何的名字
    switche = {
        0: "zero",
        1: "one",
        2: "two",
    }
    return switche.get(argument,"nothing")