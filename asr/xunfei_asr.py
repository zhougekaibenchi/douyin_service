#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/11/1 9:58
# @Author  : stce

# ******************************************** 讯飞转写接口——循环访问 ******************************************************

import hmac
import json
import time
import urllib
import base64
import hashlib
import requests
from __init__ import *

# lfasr_host = 'https://raasr.xfyun.cn/v2/api'
# # 请求的接口名
# api_upload = '/upload'
# api_get_result = '/getResult'

class RequestApi(object):
    def __init__(self, config):

        # 账号，ID
        self.appid = config["XunFei_ASR"]["Long_Form_ASR"]["appid"]
        self.secret_key = config["XunFei_ASR"]["Long_Form_ASR"]["secret_key"]
        # 接口地址
        self.lfasr_host = config["XunFei_ASR"]["upload"]["lfasr_host"]
        self.api_upload = config["XunFei_ASR"]["upload"]["api_upload"]
        self.api_get_result = config["XunFei_ASR"]["upload"]["api_get_result"]
        # 参数配置
        self.sysDicts = config["XunFei_ASR"]["upload"]["sysDicts"]
        self.duration = config["XunFei_ASR"]["upload"]["duration"]
        # 上传文件路径
        self.upload_file_path_ZMY = self.get_upload_file_path(config["HOT_Trends"]["dst_file_path"])
        self.upload_file_path_JMZ = self.get_upload_file_path(config["HOT_Trends"]["crawler_video_local_path"])
        self.ts = str(int(time.time()))
        self.signa = self.get_signa()

    def get_upload_file_path(self, url):
        "遍历指定目下的所有文件"
        file = os.listdir(url)
        filename = list()
        for f in file:
            real_url = os.path.join(url, f)
            if os.path.isfile(real_url):
                logger.info(os.path.abspath(real_url))
                filename.append(os.path.abspath(real_url))
            elif os.path.isdir(real_url):
                self.get_upload_file_path(real_url)
            else:
                logger.info("Error: 文件遍历错误")
                pass
            logger.info(real_url)
        return filename

    def get_signa(self):
        # message加密
        appid = self.appid
        secret_key = self.secret_key
        m2 = hashlib.md5()
        m2.update((appid + self.ts).encode('utf-8'))
        md5 = m2.hexdigest()
        md5 = bytes(md5, encoding='utf-8')
        # 以secret_key为key, 上面的md5为msg， 使用hashlib.sha1加密结果为signa
        signa = hmac.new(secret_key.encode('utf-8'), md5, hashlib.sha1).digest()
        signa = base64.b64encode(signa)
        signa = str(signa, 'utf-8')
        return signa

    def upload_init(self, upload_file_path):
        # 上传数据接口参数构建
        upload_file_path = upload_file_path
        file_len = os.path.getsize(upload_file_path)
        file_name = os.path.basename(upload_file_path)
        logger.info("文件upload路径：{}".format(file_name))
        param_dict = {}
        param_dict['appId'] = self.appid
        param_dict['signa'] = self.signa
        param_dict['ts'] = self.ts
        param_dict["fileSize"] = file_len
        param_dict["fileName"] = file_name
        param_dict["sysDicts"] = "advertisement"
        param_dict["duration"] = "200"
        logger.info("upload接口参数：", param_dict)
        data = open(upload_file_path, 'rb').read(file_len)
        return param_dict, data

    def upload(self, upload_file_path):
        # 上传数据
        param_dict, data = self.upload_init(upload_file_path)
        response = requests.post(url=self.lfasr_host + self.api_upload+"?"+urllib.parse.urlencode(param_dict),
                                 headers={"Content-type":"application/json"},
                                 data=data)
        logger.info("upload_url:", response.request.url)
        result = json.loads(response.text)
        logger.info("upload resp:", result)
        return result

    def download_init(self, uploadresp):
        # 获取数据接口参数构建
        orderId = uploadresp['content']['orderId']
        logger.info("文件查询ID：{}".format(orderId))
        param_dict = {}
        param_dict['appId'] = self.appid
        param_dict['signa'] = self.signa
        param_dict['ts'] = self.ts
        param_dict['orderId'] = orderId
        param_dict['resultType'] = "transfer,predict"
        logger.info("查询部分：")
        logger.info("get result参数：", param_dict)
        return param_dict

    def download(self, uploadresp):
        # 下载数据
        param_dict = self.get_upload_file_path(uploadresp)
        response = requests.post(url=self.lfasr_host + self.api_get_result + "?" + urllib.parse.urlencode(param_dict),
                                 headers={"Content-type": "application/json"})
        logger.info("get_result_url:",response.request.url)
        result = json.loads(response.text)
        logger.info("get_result resp:", result)
        return result

    def get_result(self):
        """

        """








    # def get_result(self):
    #
    #     uploadresp = self.upload()
    #     orderId = uploadresp['content']['orderId']
    #     param_dict = {}
    #     param_dict['appId'] = self.appid
    #     param_dict['signa'] = self.signa
    #     param_dict['ts'] = self.ts
    #     param_dict['orderId'] = orderId
    #     param_dict['resultType'] = "transfer,predict"
    #     print("")
    #     print("查询部分：")
    #     print("get result参数：", param_dict)
    #     status = 3
    #     # 建议使用回调的方式查询结果，查询接口有请求频率限制
    #     while status == 3:
    #         response = requests.post(url=lfasr_host + api_get_result + "?" + urllib.parse.urlencode(param_dict),
    #                                  headers={"Content-type": "application/json"})
    #         # print("get_result_url:",response.request.url)
    #         print("查询中······")
    #         result = json.loads(response.text)
    #         status = result['content']['orderInfo']['status']
    #         time.sleep(5)
    #     print("get_result resp:", result)
    #     return result




# 输入讯飞开放平台的appid，secret_key和待转写的文件路径
if __name__ == '__main__':
    api = RequestApi(appid="xxxxx",
                     secret_key="xxxxx",
                     upload_file_path=r"audio/lfasr_涉政.wav")

    api.get_result()
