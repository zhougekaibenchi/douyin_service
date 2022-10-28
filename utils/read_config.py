#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/25 15:32
# @Author  : stce
from __init__ import *
from ruamel_yaml import YAML


class Env(object):
    @classmethod
    def get(cls, env):
        logger.info("读取配置文件")
        yaml = YAML()
        if env == "dev":
            with open('config/dev.yaml', 'r', encoding='utf-8') as f:
                config = yaml.load(f)
            return config
        elif env == "stg":
            with open('config/stg.yaml', 'r', encoding='utf-8') as f:
                config = yaml.load(f)
            return config
        elif env == "prd":
            with open('config/prd.yaml', 'r', encoding='utf-8') as f:
                config = yaml.load(f)
            return config
        elif env == "test":
            # 本地测试使用
            with open('config/test.yaml', 'r', encoding='utf-8') as f:
                config = yaml.load(f)
            return config
        else:
            raise ValueError("配置文件必须为 dev/std/prd, 其中之一")

if __name__ == "__main__":
    pass
