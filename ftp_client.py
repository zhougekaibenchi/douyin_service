#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce

from __init__ import *
from ftplib import FTP
import os
import time
import datetime
from hot_tracking import hot_keyword_tracking, recall_process, data_parser
from hot_tracking.config import Config

class FTP_OP(object):
    """
    FTP文件下载类
    """
    def __init__(self, config):
        """
        :param config: 配置文件
        """
        self.current_time = str(datetime.date.today())
        self.host = config["FTP_Sever"]["host"]
        self.username = config["FTP_Sever"]["username"]
        self.password = config["FTP_Sever"]["password"]
        self.port = int(config["FTP_Sever"]["port"])
        self.buffer_size = config["FTP_Sever"]["buffer_size"]

    def ftp_connect(self):
        ftp = FTP()
        ftp.set_debuglevel(1) # 调试模式设置
        ftp.connect(host=self.host, port=self.port)
        ftp.login(self.username, self.password)
        ftp.set_pasv(True)  #主动模式，被动模式调整
        logger.info(ftp.getwelcome())
        ftp.cwd(self.current_time)
        file_list = ftp.nlst()
        print(file_list)
        return ftp

    def download_file(self, local_path, sever_path):
        raise NotImplementedError()

    def upload_file(self, local_path, server_path):
        raise NotImplementedError()

    def scaner_file(self, url):
        "遍历指定目下的所有文件"
        file = os.listdir(url)
        filename = list()
        for f in file:
            real_url = os.path.join(url, f)
            if os.path.isfile(real_url):
                logger.info(os.path.abspath(real_url))
                filename.append(os.path.abspath(real_url))
            elif os.path.isdir(real_url):
                self.scaner_file(real_url)
            else:
                logger.info("Error: 文件遍历错误")
                pass
            logger.info(real_url)
        return filename


# *************************************************  ZMY  **************************************************************

class FTP_Updata(FTP_OP):

    def __init__(self, config):
        """
        日常抖音账号的数据更新
        :param ftp_file_path: ftp下载文本文件爬虫数据
        :param severMP3file_path: ftp下载服务端爬虫数据
        :param localMP4file_path: 本地存放路径
        """
        super(FTP_Updata, self).__init__(config)
        self.server_txt_path = self.current_time + config["Douyin_Updata"]["server_txt_path"]
        self.local_txt_path = self.current_time + config["Douyin_Updata"]["local_txt_path"]
        self.severMP3file_path = self.current_time + config["Douyin_Updata"]["severMP3file_path"]
        self.localMP3file_path = config["Douyin_Updata"]["localMP3file_path"]


    def download_file(self):

        logger.info("ftp数据传输开始")
        self.ftp = self.ftp_connect()
        # (1) 传输文本文件
        f = open(self.local_txt_path, "wb")
        self.ftp.retrbinary('RETR %s' % self.server_txt_path, f.write, self.buffer_size)

        # (2) 传输音频文件
        file_list = self.ftp.nlst(self.severMP3file_path)
        logger.info(file_list)
        for file_name in file_list:
            ftp_file = os.path.join(self.severMP3file_path, file_name)
            logger.info("服务端ftp_file读取路径: " + ftp_file)
            local_file = os.path.join(self.localMP3file_path, file_name)
            logger.info("客户端local_file存储路径: " + local_file)
            f = open(local_file, "wb")
            self.ftp.retrbinary('RETR %s'%ftp_file, f.write, self.buffer_size)
            f.close()
            logger.info("文件下载成功：" + file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"ftp数据下载完毕")
        logger.info("******************************DouyinData ZMY Get Finish*****************************************")
        self.ftp.quit()

    def upload_file(self):

        self.ftp = self.ftp_connect()
        file_list = self.scaner_file(sever_path)
        for file_name in file_list:
            f = open(file_name, "rb")
            file_name = os.path.split(file_name)[-1]
            self.ftp.storbinary('STOR %s' % file_name, f, self.buffer_size)
            logger.info('成功上传文件： "%s"' % file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"文件全部上传完毕")
        self.ftp.quit()












# ****************************************************  JMZ  ***********************************************************
class FTP_HOTTrends(FTP_OP):
    """
    美芝老师热点数据更新
    :param hottrends_sever_path: 下载ftp热榜数据地址
    :param hottrends_local_path: 本地存储热榜
    :param searchitem_sever_path: 上传搜索词ftp存储地址
    :param searchitem_local_path: 本地搜索词本地存储地址
    :param crawler_local_path: 本地爬虫数据存储地址
    :param crawler_sever_path: 下载ftp爬虫数据地址
    :param crawlervideo_list_local_path: 本地ftp爬虫音频数据列表地址
    :param crawlervideo_list_sever_path: 上传ftp爬虫音频数据列表地址
    :param crawler_video_local_path: 本地视频数据地址
    :param crawler_video_sever_path: 下载ftp视频数据地址
    """
    def __init__(self, config):
        self.current_time = str(datetime.date.today())
        self.base_path = config["HOT_Trends"]["base_path"]
        self.base_asr_path = config["Douyin_Updata"]["base_asr_path"]
        # 下载本地热榜数据
        self.hottrends_local_path = self.base_path + self.current_time + config["HOT_Trends"]["hottrends_local_path"]
        self.hottrends_sever_path = self.current_time + config["HOT_Trends"]["hottrends_sever_path"]
        # 确定搜索词
        self.searchitem_local_path = self.base_path + self.current_time + config["HOT_Trends"]["searchitem_local_path"]
        self.searchitem_sever_path = self.current_time + config["HOT_Trends"]["searchitem_sever_path"]
        # 下载视频文本数据
        self.crawler_local_path = self.base_path + self.current_time + config["HOT_Trends"]["crawler_local_path"]
        self.crawler_sever_path = self.current_time + config["HOT_Trends"]["crawler_sever_path"]
        # 确定需要下载的视频列表
        self.crawlervideo_list_local_path = self.base_path + self.current_time + config["HOT_Trends"]["crawlervideo_list_local_path"]
        self.crawlervideo_list_sever_path = self.current_time + config["HOT_Trends"]["crawlervideo_list_sever_path"]
        # 将音频数据拷贝到本地服务器
        self.crawler_video_local_path = self.base_asr_path + self.current_time + config["HOT_Trends"]["crawler_video_local_path"]
        self.crawler_video_sever_path = self.current_time + config["HOT_Trends"]["crawler_video_sever_path"]
        super(FTP_HOTTrends, self).__init__(config)
        self.ftp = self.ftp_connect()


    def download_file(self, local_path=None, sever_path=None):
        """
        批量下载抖音数据
        """
        file_list = self.ftp.nlst()
        logger.info("ftp数据传输开始")
        logger.info(file_list)
        for file_name in file_list:
            ftp_file = os.path.join(local_path, file_name)
            logger.info("服务端ftp_file读取路径: " + ftp_file)
            local_file = os.path.join(sever_path, file_name)
            logger.info("客户端local_file存储路径: " + local_file)
            f = open(local_file, "wb")
            self.ftp.retrbinary('RETR %s'%ftp_file, f.write, self.buffer_size)
            f.close()
            logger.info("文件下载成功：" + file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"ftp数据下载完毕")

    def download_single_file(self, local_path=None, sever_path=None):
        """
        单个下载抖音数据
        """
        f = open(self.local_ftpfile_path, "wb")
        self.ftp.retrbinary('RETR %s' % self.ftp_file_path, f.write, self.buffer_size)

    def upload_file(self, local_path=None, sever_path=None):

        file_list = self.scaner_file(local_path)
        for file_name in file_list:
            f = open(file_name, "rb")
            file_name = os.path.split(file_name)[-1]
            self.ftp.storbinary('STOR %s' % file_name, f, self.buffer_size)
            logger.info('成功上传文件： "%s"' % file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"文件全部上传完毕")

    def data_collect(self):
        """
        （1）下载热榜数据
        （2）todo
        （3）上传搜索词
        （4）下载爬虫数据
        （5）上传爬虫数据列表
        （6）下载视频数据
        """
        self.download_file(self.hottrends_local_path, self.hottrends_sever_path) # 取热榜数据

        # 生成热搜词
        config = Config()
        douyinDataset = data_parser.DouyinDataset(config)
        douyinDataset.merge_dataset()    # 将json格式转换成xls

        keywordMining = hot_keyword_tracking.KeywordMiningByTags(config)
        keywordMining.merge_keywords()  # 是否使用热门视频关键短语数据

        self.upload_file(self.searchitem_local_path, self.searchitem_sever_path)    # 将热搜词传送给爬虫端
        self.download_file(self.crawler_local_path, self.crawler_sever_path)       # 从爬虫端下载搜索热词视频数据

        # 召回、排序热搜词相关视频
        recall = recall_process.RecallSearchDataset(config)
        recallDataset = recall.recall_dataset_by_insurances()   # 召回
        recall.rank_dataset_by_count(recallDataset)  # 排序

        self.upload_file(self.crawlervideo_list_local_path, self.crawlervideo_list_sever_path)    # 将整理结果数据传送给爬虫端
        self.download_file(self.crawler_video_local_path, self.crawler_video_sever_path)     # 从爬虫端下载对应视频文案
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"热点数据下载完毕")   #
        self.ftp.quit()
        logger.info("******************************DouyinData JMZ Get Finish*****************************************")


if __name__ == '__main__':
    pass