#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 10:02
# @Author  : stce

import gc
import sys
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
        ftp_update.download_account()

        # 美芝老师热点数据更新
        ftp_hottrends = FTP_HOTTrends(config)
        ftp_hottrends.data_collect()

        # 数据ASR输出
        asr_request = RequestApi(config, FTP_Updata.get_time())
        asr_request.get_result()

        # 基于视频文案进行过滤，并输出最终结果
        ftp_hottrends.data_collect_by_content()    # 基于视频文案进行过滤，并输出最终结果

        # 文案改写
        ftp_hottrends.video_content_rewrite()


    except Exception as ex:
        logger.error("pipeline出现错误")
        raise ex

    del ftp_update, ftp_hottrends
    gc.collect()
    logger.info("******************************Pipeline Finish****************************************")

if __name__ == "__main__":
    env = sys.argv[1] #"dev"
    # env = 'dev'
    douyin_pipeline(env)