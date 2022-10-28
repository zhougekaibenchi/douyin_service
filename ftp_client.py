#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce

from __init__ import *
from ftplib import FTP
import os
import time


class FTP_OP(object):
    """
    FTP文件下载类
    """
    def __init__(self, config):
        """
        (1) 初始化ftp所需的配置文件
        :param host: ftp主机ip, '14.116.177.18'
        :param username: ftp用户名, 'username1'
        :param password: ftp密码, '1PASSWORD'
        :param port: ftp端口 （默认21）
        :param buffer_size: 204800  #默认是8192

        (2) 路径文件配置
        :param ftp_file_path: ftp下载文件路径
        :param dst_file_path: 本地存放路径

        """
        self.host = config["FTP_Sever"]["host"]
        self.username = config["FTP_Sever"]["username"]
        self.password = config["FTP_Sever"]["password"]
        self.port = int(config["FTP_Sever"]["port"])
        self.buffer_size = config["FTP_Sever"]["buffer_size"]
        self.ftp_file_path = config["FTP_Sever"]["ftp_file_path"]
        self.dst_file_path = config["FTP_Sever"]["dst_file_path"]

    def ftp_connect(self):

        ftp = FTP()
        ftp.set_debuglevel(1) # 调试模式设置
        ftp.connect(host='127.0.0.1', port=self.port)
        ftp.login(self.username, self.password)
        ftp.set_pasv(False)  #主动模式，被动模式调整
        logger.info(ftp.getwelcome())
        return ftp

    def download_file(self):
        """
        从ftp服务端，下载文件到本地
        """
        logger.info("ftp数据传输开始")
        self.ftp = self.ftp_connect()
        file_list = self.ftp.nlst(self.ftp_file_path)
        logger.info(file_list)
        for file_name in file_list:
            ftp_file = os.path.join(self.ftp_file_path, file_name)
            logger.info("服务端ftp_file读取路径: " + ftp_file)
            #write_file = os.path.join(dst_file_path, file_name)
            local_file = self.dst_file_path + file_name
            logger.info("客户端local_file存储路径: " + local_file)
            f = open(local_file, "wb")
            self.ftp.retrbinary('RETR %s'%ftp_file, f.write, self.buffer_size)
            f.close()
            logger.info("文件下载成功：" + file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"ftp数据下载完毕")
        self.ftp.quit()

    def upload_file(self, filepath):

        self.ftp = self.ftp_connect()
        file_list = self.scaner_file(filepath)
        for file_name in file_list:
            f = open(file_name, "rb")
            file_name = os.path.split(file_name)[-1]
            self.ftp.storbinary('STOR %s' % file_name, f, self.buffer_size)
            logger.info('成功上传文件： "%s"' % file_name)
        logger.info(time.strftime('%Y%m%d', time.localtime(time.time()))+"文件全部上传完毕")
        self.ftp.quit()

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

if __name__ == '__main__':
    pass