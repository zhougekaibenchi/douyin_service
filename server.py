#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 9:35
# @Author  : stce

import uvicorn
from utils.read_config import Env
from asr.xunfei_asr import RequestApi
from fastapi import FastAPI
from utils.utils import server_log
app = FastAPI()

"""
(1) /xunfei_lfasr 提供给讯飞当作回调端口，接收已经完成的ASR数据并存储，
(2) /health 检查ASR接收服务是否正常
"""

# @app.get("/xunfei_lfasr")
"""
目前采用轮询的方式获取数据，没有采用回调函数获取
"""
# def xunfei_lfasr(orderId, status, resultType=None):
#     """
#     (1) 下载ASR数据
#     (2) 数据后处理
#     (3) 数据保存
#     """
#     if status == "1":
#         asr_download = RequestApi(config)
#         result = asr_download.download(orderId)
#         asr_txt = asr_download.post_process(result)
#         asr_download.save_asrdata(asr_txt)
#     else:
#         logger.info("ASR数据接收错误")
#     return {"status": "OK"}


@app.get("/health")
def health():
    return {"status": "UP"}


if __name__ == '__main__':
    env = "dev" #sys.argv[1]
    config = Env.get(env)
    logger = server_log(config["Log_Path"]["sever_log_path"])
    logger.info("服务ASR数据接收服务启动..........")
    logger.info("服务启动ip：0.0.0.0:8001")
    uvicorn.run(app, host='127.0.0.1', port=8010)