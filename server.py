#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 9:35
# @Author  : stce

import uvicorn
from utils.utils import write_complete_list, read_complete_list
from utils.read_config import Env
from asr.xunfei_asr import RequestApi
from fastapi import FastAPI
from utils.utils import server_log
app = FastAPI()

"""
(1) /xunfei_lfasr 提供给讯飞当作回调端口，接收已经完成的ASR数据并存储，
(2) /xunfei_lfasr_listener 监听已经完成的ASR数据
(3) /health 检查ASR接收服务是否正常
"""


@app.get("/xunfei_lfasr")
def xunfei_lfasr(orderId, status, resultType=None):
    #
    if status == "1":
        asr_download = RequestApi(config)
        result = asr_download.download(orderId)
        asr_download.post_process(result)
        write_complete_list(orderId)
    else:
        logger.info("ASR数据接收错误")
    return {"status": "OK"}

@app.get("/xunfei_lfasr_listener")
def asr_listener(total_nums):
    complete_nums = read_complete_list()
    logger.info("总共数据量{}".format(len(complete_nums)/total_nums))



@app.get("/health")
def health():
    return {"status": "UP"}


if __name__ == '__main__':
    env = "dev" #sys.argv[1]
    logger = server_log(env)
    logger.info("服务ASR数据接收服务启动..........")
    logger.info("服务启动ip：0.0.0.0:8001")
    config = Env.get(env)
    uvicorn.run(app, host='127.0.0.1', port=8001)