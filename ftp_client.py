#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce

from __init__ import *
from ftplib import FTP
import os
import time
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
        self.host = config["FTP_Sever"]["host"]
        self.username = config["FTP_Sever"]["username"]
        self.password = config["FTP_Sever"]["password"]
        self.port = int(config["FTP_Sever"]["port"])
        self.buffer_size = config["FTP_Sever"]["buffer_size"]

    def ftp_connect(self):
        ftp = FTP()
        ftp.set_debuglevel(1) # 调试模式设置
        ftp.connect(host='127.0.0.1', port=self.port)
        ftp.login(self.username, self.password)
        ftp.set_pasv(False)  #主动模式，被动模式调整
        logger.info(ftp.getwelcome())
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


class FTP_Updata(FTP_OP):

    def __init__(self, config):
        """
        日常抖音账号的数据更新
        :param ftp_file_path: ftp下载文件路径
        :param dst_file_path: 本地存放路径
        """
        self.ftp_file_path = config["Douyin_Updata"]["ftp_file_path"]
        self.dst_file_path = config["Douyin_Updata"]["dst_file_path"]
        super(FTP_Updata, self).__init__(config)

    def download_file(self, local_path=None, sever_path=None):

        logger.info("ftp数据传输开始")
        self.ftp = self.ftp_connect()
        file_list = self.ftp.nlst(self.ftp_file_path)
        logger.info(file_list)
        for file_name in file_list:
            ftp_file = os.path.join(self.ftp_file_path, file_name)
            logger.info("服务端ftp_file读取路径: " + ftp_file)
            local_file = os.path.join(self.dst_file_path, file_name)
            logger.info("客户端local_file存储路径: " + local_file)
            f = open(local_file, "wb")
            self.ftp.retrbinary('RETR %s'%ftp_file, f.write, self.buffer_size)
            f.close()
            logger.info("文件下载成功：" + file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"ftp数据下载完毕")
        logger.info("******************************DouyinData ZMY Get Finish*****************************************")
        self.ftp.quit()

    def upload_file(self, local_path=None, sever_path=None):

        self.ftp = self.ftp_connect()
        file_list = self.scaner_file(sever_path)
        for file_name in file_list:
            f = open(file_name, "rb")
            file_name = os.path.split(file_name)[-1]
            self.ftp.storbinary('STOR %s' % file_name, f, self.buffer_size)
            logger.info('成功上传文件： "%s"' % file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"文件全部上传完毕")
        self.ftp.quit()


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
        self.hottrends_local_path = config["HOT_Trends"]["hottrends_local_path"]
        self.hottrends_sever_path = config["HOT_Trends"]["hottrends_sever_path"]

        self.searchitem_local_path = config["HOT_Trends"]["searchitem_local_path"]
        self.searchitem_sever_path = config["HOT_Trends"]["searchitem_sever_path"]

        self.crawler_local_path = config["HOT_Trends"]["crawler_local_path"]
        self.crawler_sever_path = config["HOT_Trends"]["crawler_sever_path"]

        self.crawlervideo_list_local_path = config["HOT_Trends"]["crawlervideo_list_local_path"]
        self.crawlervideo_list_sever_path = config["HOT_Trends"]["crawlervideo_list_sever_path"]

        self.crawler_video_local_path = config["HOT_Trends"]["crawler_video_local_path"]
        self.crawler_video_sever_path = config["HOT_Trends"]["crawler_video_sever_path"]

        self.ftp = self.ftp_connect()
        super(FTP_HOTTrends, self).__init__(config)

    def download_file(self, local_path=None, sever_path=None):
        """
        从ftp服务端，下载日常抖音数据增量文件
        """
        logger.info("ftp数据传输开始")
        file_list = self.ftp.nlst(local_path)
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