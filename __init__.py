#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/26 9:51
# @Author  : stce

import os
import redis
import logging.handlers

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# 日志文件记录
filename = "log/cron.log"
os.makedirs(os.path.dirname(filename), exist_ok=True)

file_handler = logging.handlers.TimedRotatingFileHandler(filename, 'D', 1, 3, encoding='utf8')
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(funcName)s() - %(message)s'
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 重定向到terminal, 日志输出
cmd_handler = logging.StreamHandler()
cmd_handler.setLevel(logging.DEBUG)
formatter_cmd = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(funcName)s() - %(message)s'
)
cmd_handler.setFormatter(formatter_cmd)
logger.addHandler(cmd_handler)
