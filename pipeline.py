#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 10:02
# @Author  : stce

import gc
import logger
from __init__ import *
from asr.xunfei_asr import RequestApi
from ftp_client import FTP_Updata, FTP_HOTTrends
from utils.read_config import Env


def douyin_pipeline(env):

    try:
        config = Env.get(env)
        # 抖音账号数据更新
        ftp_update = FTP_Updata(config)
        ftp_update.download_file()
        # 美芝老师热点数据更新
        ftp_hottrends = FTP_HOTTrends(config)
        ftp_hottrends.data_collect()
        # 数据ASR输出
        asr_request = RequestApi(config)
        upload_list = asr_request.upload_data()
        # 等待接收ASR返回，并存储到到对应位置


        # todo ZMY其他逻辑


    except Exception as ex:
        logger.error("pipeline出现错误")
        raise ex

    del ftp_update, ftp_hottrends
    gc.collect()
    logger.info("Pipeline Finish")

if __name__ == "__main__":
    env = "dev"
    douyin_pipeline(env)