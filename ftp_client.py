#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce

from __init__ import *
from ftplib import FTP
import ftplib
import os
import time
import tqdm
import datetime
import json
import pandas as pd
import requests
import copy
from tqdm import tqdm
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
        # self.current_time = '2022-11-29'
        self.host = config["FTP_Sever"]["host"]
        self.username = config["FTP_Sever"]["username"]
        self.password = config["FTP_Sever"]["password"]
        self.port = int(config["FTP_Sever"]["port"])
        self.buffer_size = config["FTP_Sever"]["buffer_size"]

    #@staticmethod
    def get_time(self):
        return self.current_time

    def ftp_connect(self):
        ftp = FTP()
        ftp.set_debuglevel(1) # 调试模式设置
        ftp.connect(host=self.host, port=self.port)
        ftp.login(self.username, self.password)
        ftp.set_pasv(True)  #主动模式，被动模式调整
        logger.info(ftp.getwelcome())
        return ftp

    def download_file(self, local_path, sever_path):
        raise NotImplementedError()

    def upload_file(self, local_path, server_path):
        raise NotImplementedError()

    def make_dir(self):
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
        self.server_txt_path = config["Douyin_Updata"]["server_txt_path"]
        self.local_txt_path = config["Douyin_Updata"]["base_asr_path"] + self.current_time + config["Douyin_Updata"]["local_txt_path"]
        self.severMP3file_path = config["Douyin_Updata"]["severMP3file_path"]
        self.localMP3file_path = config["Douyin_Updata"]["base_asr_path"] + self.current_time + config["Douyin_Updata"]["localMP3file_path"]
        self.make_dir()

        self.ftp = self.ftp_connect()

    def make_dir(self):
        isExists = os.path.exists(self.localMP3file_path)
        if not isExists:
            os.makedirs(self.localMP3file_path)
            logger.info("创建目录成功： {}".format(self.localMP3file_path))
        else:
            logger.info("{} 目录已经存在".format(self.localMP3file_path))

    def check(self, file_list):
        """
        当中断后重启检查当前目录下的文件
        """
        file_list_c = self.scaner_file(self.localMP3file_path)
        file_list_c = [item.split("\\")[-1] for item in file_list_c] # linux为/
        for item in file_list_c:
            if item in file_list:
                file_list.remove(item)
        return file_list

    def download_file(self):
        logger.info("ftp数据传输开始")

        isBreak = True
        while isBreak:
            try:
                self.ftp.cwd('/')
                self.ftp.cwd(self.current_time)
                f = open(self.local_txt_path, "wb")
                self.ftp.retrbinary('RETR %s' % self.server_txt_path, f.write, self.buffer_size)
                f.close()
                isBreak = False
            except Exception as ex:
                print(ex)
                logger.info("无法下载文本数据，请检查服务端文本数据路径! " + self.server_txt_path)


            if isBreak:
                time.sleep(5 * 60)

    def download_path(self):
        logger.info("ftp数据传输开始")
        if not os.path.exists(self.localMP3file_path):
            os.makedirs(self.localMP3file_path)

        localMP3files = os.listdir(self.localMP3file_path)


        isBreak = True
        while isBreak:
            try:
                self.ftp.cwd('/')
                self.ftp.cwd(self.current_time)
                file_list = self.ftp.nlst(self.severMP3file_path)
                file_list = self.check(file_list)
                while file_list:
                    for file_name in tqdm(file_list, desc='asr音频文件下载'):
                        if file_name in localMP3files:
                            continue
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
                    isBreak = False
                    break
            except:
                logger.info("无法下载视频数据，请检查视频数据路径! " + self.severMP3file_path)

            if isBreak:
                time.sleep( 5 * 60)


    def download_account(self):

        # (1) 传输文本文件
        self.download_file()

        # (2) 传输音频文件
        self.download_path()


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
        super(FTP_HOTTrends, self).__init__(config)

        self.base_path = config["HOT_Trends"]["base_path"]
        self.base_asr_path = config["Douyin_Updata"]["base_asr_path"]
        self.sever_base_path = config["HOT_Trends"]["sever_base_path"]
        # 下载本地热榜数据
        self.hotvideo_local_path = self.base_path + self.current_time + config["HOT_Trends"]["hotvideo_local_path"]
        self.hotvideo_sever_path = self.sever_base_path + self.current_time + config["HOT_Trends"]["hotvideo_sever_path"]
        # 确定搜索词
        self.hotkeywords_local_path = self.base_path + self.current_time + config["HOT_Trends"]["hotkeywords_local_path"]
        self.hotkeywords_sever_path = self.sever_base_path + self.current_time + config["HOT_Trends"]["hotkeywords_sever_path"]
        # 下载视频文本数据
        self.searchvideo_local_path = self.base_path + self.current_time + config["HOT_Trends"]["searchvideo_local_path"]
        self.searchvideo_sever_path = self.sever_base_path + self.current_time + config["HOT_Trends"]["searchvideo_sever_path"]
        # 确定需要下载的视频列表
        self.recallvideo_local_path = self.base_path + self.current_time + config["HOT_Trends"]["recallvideo_local_path"]
        self.recallvideo_sever_path = self.sever_base_path + self.current_time + config["HOT_Trends"]["recallvideo_sever_path"]
        # 将音频数据拷贝到本地服务器
        self.crawler_video_local_path = self.base_asr_path + self.current_time + config["HOT_Trends"]["crawler_video_local_path"]
        self.crawler_video_sever_path = self.sever_base_path + self.current_time + config["HOT_Trends"]["crawler_video_sever_path"]

        # 热门短语中间文件路径
        self.hotkeywords_tmp_local_path = self.base_path + self.current_time + config["HOT_Trends"]["hotkeywords_tmp_local_path"]
        # 最终输入视频文件路径
        self.hottracking_result_local_path = self.base_path + self.current_time + config["HOT_Trends"]["hottracking_result_local_path"]
        # 视频文案改写文件路径
        self.rewriter_local_path = self.base_path + self.current_time + config['HOT_Trends']['rewriter_local_path']
        # 视频文案路径
        self.video_content_path = self.base_asr_path + self.current_time + config['HOT_Trends']['localASRfile_path']

        # 产品下载、上传文件路径
        self.hot_trends_dowload_path = config['HOT_Trends']['hot_trends_dowload_path']
        self.hot_trends_upload_path = config['HOT_Trends']['hot_trends_upload_path']
        if not os.path.exists(self.hot_trends_dowload_path):
            os.makedirs(self.hot_trends_dowload_path)

        if not os.path.exists(self.hot_trends_upload_path):
            os.makedirs(self.hot_trends_upload_path)


        self.hotConfig = Config(self.hotvideo_local_path,           # 热榜数据路径
                                self.hotkeywords_local_path,        # 挖掘热门词数据路径
                                self.hotkeywords_tmp_local_path,    # 挖掘热门词中间文件
                                self.searchvideo_local_path,        # 搜索词检索视频数据路径
                                self.recallvideo_local_path,        # 视频召回数据路径
                                self.video_content_path,            # 视频文案数据
                                self.hottracking_result_local_path,   # 最终输入结果数据
                                self.rewriter_local_path) # 最终文案改写数据

        self.ftp = self.ftp_connect()


    def download_file(self, local_path=None, sever_path=None):
        """
        批量下载抖音数据（下载整个目录）
        """
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        loacl_file_list = os.listdir(local_path)

        isBreak = True


        # 切换目录
        while isBreak:
            try:
                self.ftp.cwd('/')
                self.ftp.cwd(sever_path)
                break

            except ftplib.error_temp as e:
                self.ftp = self.ftp_connect()
                logger.info("远程服务连接超时！")
                time.sleep(5 * 60)

            except Exception as ex:
                logger.info(sever_path + "路径不存在！")
                time.sleep(1 * 60)


        while isBreak:
            try:
                file_list = self.ftp.nlst()
                logger.info("ftp数据传输开始")
                logger.info(file_list)
                if not file_list:
                    time.sleep(1 * 60)
                    continue

                for file_name in tqdm(file_list, desc="视频数据下载"):
                    if file_name in loacl_file_list:
                        continue
                    ftp_file = os.path.join(sever_path, file_name)
                    logger.info("服务端ftp_file读取路径: " + ftp_file)
                    local_file = os.path.join(local_path, file_name)
                    logger.info("客户端local_file存储路径: " + local_file)
                    if not os.path.exists(local_path):
                        os.makedirs(local_path)
                    f = open(local_file, "wb")
                    self.ftp.retrbinary('RETR %s'%ftp_file, f.write, self.buffer_size)
                    f.close()
                    logger.info("文件下载成功：" + file_name)
                logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"ftp数据下载完毕")
                break


            except ftplib.error_temp as e:
                logger.info("远程服务连接超时！")
                self.ftp = self.ftp_connect()
                # self.ftp.cwd('/')
                self.ftp.cwd(sever_path)
                time.sleep(5 * 60)


            except Exception as ex:
                logger.info("数据下载失败：")
                time.sleep(1 * 60)



    def download_single_file(self, local_path=None, sever_path=None):
        """
        单个下载抖音数据（下载单个文件夹）
        """
        f = open(local_path, "wb")
        self.ftp.retrbinary('RETR %s' % sever_path, f.write, self.buffer_size)

    def upload_file(self, local_path=None, sever_path=None):
        '''
        批量上传文件（上传整个文件夹）
        :param local_path:
        :param sever_path:
        :return:
        '''
        try:
            self.ftp.cwd('/')
            self.ftp.cwd(sever_path)
        except:
            self.ftp.mkd(sever_path)
            self.ftp.cwd(sever_path)
        file_list = self.scaner_file(local_path)
        for file_name in file_list:
            f = open(file_name, "rb")
            file_name = os.path.split(file_name)[-1]
            ftp_file = os.path.join(sever_path, file_name)
            self.ftp.storbinary('STOR %s' % file_name, f, self.buffer_size)
            logger.info('成功上传文件： "%s"' % ftp_file)
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
        # 下载热榜数据
        self.download_file(self.hotvideo_local_path, self.hotvideo_sever_path) # 取热榜数据

        # 生成热搜词
        douyinDataset = data_parser.DouyinDataset(self.hotConfig)
        douyinDataset.merge_dataset()    # 将json格式转换成xls

        keywordMining = hot_keyword_tracking.KeywordMiningByTags(self.hotConfig)
        keywordMining.merge_keywords()  # 是否使用热门视频关键短语数据

        # 将热搜词传送给爬虫端，从爬虫端下载搜索热词视频数据
        self.upload_file(self.hotkeywords_local_path, self.hotkeywords_sever_path)    # 将热搜词传送给爬虫端
        self.download_file(self.searchvideo_local_path, self.searchvideo_sever_path)       # 从爬虫端下载搜索热词视频数据

        # 召回、排序热搜词相关视频
        recall = recall_process.RecallSearchDataset(self.hotConfig)
        recallDataset = recall.recall_dataset_by_insurances()   # 召回
        # recallDataset = recall.recall_dataset_by_bm25(recallDataset)
        # recallDataset = recall.recall_by_bert()
        recall.rank_dataset_by_count(recallDataset)  # 排序

        self.upload_file(self.recallvideo_local_path, self.recallvideo_sever_path)    # 将整理结果数据传送给爬虫端

        self.download_file(self.crawler_video_local_path, self.crawler_video_sever_path)     # 从爬虫端下载对应视频文案

        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"热点数据下载完毕")   #
        self.ftp.quit()
        logger.info("******************************DouyinData JMZ Get Finish*****************************************")


    def data_collect_by_content(self):
        '''根据视频文案进行过滤，并排序'''

        recall = recall_process.RecallSearchDataset(self.hotConfig)
        resultDataset = recall.merge_video_content()
        filename = 'douyin_recall_video_' + self.current_time + '.xlsx'
        resultDataset.to_excel(os.path.join(self.hot_trends_dowload_path, filename), index=False)
        logger.info("******************************Filter By Video Content Finished*****************************************")


    def api_rewriter(self, text):
        '''调用改写接口提取改写文案'''
        self.hotConfig.rewriter_params['text'] = text
        rewriterText = requests.post(self.hotConfig.rewriter_url, data=json.dumps(self.hotConfig.rewriter_params), headers=self.hotConfig.headers)
        rewriterText = rewriterText.json()
        if rewriterText.get('data') and rewriterText['data'].get('output'):
            print("input..: ", rewriterText['data']['input'])
            print("output: ", rewriterText['data']['output'])
            return rewriterText['data']['output']

        return ["", ""]


    def video_content_rewrite(self):
        '''对视频文案进行改写'''

        logger.info("API视频文案改写开始")
        isBreak = True
        rewriterDataset = pd.DataFrame([])

        while isBreak:
            try:
                file_list = os.listdir(self.hot_trends_upload_path)
                for filename in file_list:
                    if self.current_time in filename:
                        file = os.path.join(self.hot_trends_upload_path, filename)
                        df = pd.read_excel(file)
                        for row in tqdm(df.iterrows(), desc='文案改写'):
                            rewriterContent = self.api_rewriter(row[1]['content'])
                            row[1]['rewriter'] = rewriterContent[0]
                            row[1]['rwScore'] = rewriterContent[1]

                            rewriterDataset = rewriterDataset.append(row[1], ignore_index=True)

                        rewriterFile = os.path.join(self.hot_trends_dowload_path, 'rewriter_' + filename)
                        rewriterDataset.to_excel(rewriterFile, index=False)

                        logger.info("视频文案改写完成！")

                        isBreak = False

            except Exception as ex:
                logger.error("文案改写报错：", ex)

            if isBreak:
                time.sleep(60)







if __name__ == '__main__':
    pass