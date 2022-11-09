#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 13:39
# @Author  : stce
import os
import logging.handlers

def memory_recycling(event):
    for i in range(event):
        del event


def server_log(filename):

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_handler = logging.handlers.TimedRotatingFileHandler(filename, 'D', 1, 7, encoding='utf8')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(funcName)s() - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def write_complete_list(orderId):
    with open("tmp.txt" , "w", encoding="utf-8") as w:
        w.writelines(orderId + "\n")
        w.close()

def read_complete_list():
    with open("tmp.txt", "r", encoding="utf-8") as w:
        lines = w.readlines()
        w.close()
    return len(lines)


