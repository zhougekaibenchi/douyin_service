#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 9:35
# @Author  : stce

import uvicorn
import os
import sys
import logging.handlers
from fastapi import FastAPI
from __init__ import *
app = FastAPI()

"""
(1) /xunfei_lfasr 提供给讯飞当作回调端口，接收已经完成的ASR数据并存储，
(2) /xunfei_lfasr_listener 监听已经完成的ASR数据
(3) /health 检查ASR接收服务是否正常
"""

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


@app.get("/xunfei_lfasr")
def xunfei_lfasr():
    # todo



    return {"status": "OK"}

@app.get("/xunfei_lfasr_listener")
def asr_listener():
    pass



@app.get("/health")
def health():
    return {"status": "UP"}




if __name__ == '__main__':
    filename = sys.argv[1]
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    file_handler = logging.handlers.TimedRotatingFileHandler(filename, 'D', 1, 7, encoding='utf8')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(funcName)s() - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("服务ASR数据接收服务启动..........")
    logger.info("服务启动ip：0.0.0.0:8001")
    uvicorn.run(app, host='0.0.0.0', port=8001)