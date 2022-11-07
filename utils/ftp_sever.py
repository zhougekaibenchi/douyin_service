#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce

"""
本地启动ftp服务端，测试使用
"""

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# 实例化DummyAuthorizer来创建ftp用户
authorizer = DummyAuthorizer()
# 参数：用户名，密码，目录，权限
authorizer.add_user('username1', '1PASSWORD', r'D:/抖音数据传输分析/douyin_service/douyin_service/data', perm='elradfmwMT')
handler = FTPHandler
handler.authorizer = authorizer
# 参数：IP，端口，handler
server = FTPServer(('127.0.0.1', 21), handler)
server.serve_forever()

# authorizer.add_user('username2', 'PASSWORD', 'Directory path', perm='elradfmwMT')
# 匿名登录
# authorizer.add_anonymous('/home/nobody')