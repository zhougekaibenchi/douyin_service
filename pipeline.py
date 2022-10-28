#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/27 10:02
# @Author  : stce

import gc
import logger
from __init__ import *
from ftp_client import FTP_OP
from utils.read_config import Env

def douyin_data_updata(env):

    try:
        config = Env.get(env)
        ftp = FTP_OP(config)
        ftp.download_file()

        #todo meizhi
        data = ftp.upload_file()# 取热榜 stce
        process() # MZ
        ftp.upload_file() # 上传搜索词stce
        ftp.download_file() # 文本数据 stce
        ftp.upload_file() # 上传文件stce
        ftp.download_file() # 视频数据






    except Exception as ex:
        logger.error("pipeline出现错误")
        raise ex

    del ftp
    gc.collect()
    logger.info("pipeline完成")

if __name__ == "__main__":
    env = "test"
    douyin_data_updata(env)