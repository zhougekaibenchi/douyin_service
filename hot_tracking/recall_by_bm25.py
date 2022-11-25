#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: juzipi
@file: bm25.py
@time:2022/04/16
@description:
"""
import math
import os
import jieba
import pickle
import logging
import pandas as pd
import numpy as np

jieba.setLogLevel(log_level=logging.INFO)


class BM25Param(object):
    def __init__(self, f, df, idf, length, avg_length, docs_list, line_length_list,k1=1.5, k2=1.0,b=0.75):
        """
        :param f:
        :param df:
        :param idf:
        :param length:
        :param avg_length:
        :param docs_list:
        :param line_length_list:
        :param k1: 可调整参数，[1.2, 2.0]
        :param k2: 可调整参数，[1.2, 2.0]
        :param b:
        """
        self.f = f
        self.df = df
        self.k1 = k1
        self.k2 = k2
        self.b = b
        self.idf = idf
        self.length = length
        self.avg_length = avg_length
        self.docs_list = docs_list
        self.line_length_list = line_length_list

    def __str__(self):
        return f"k1:{self.k1}, k2:{self.k2}, b:{self.b}"


class BM25(object):
    _stop_words = []

    def __init__(self, config):
        self.config = config
        self.param = self._build_param()
        # self.param: BM25Param = self._load_param()

    def _load_stop_words(self):
        if not os.path.exists(self.config.stopwords_file):
            raise Exception(f"system stop words: {self.config.stopwords_file} not found")
        stop_words = []
        with open(self.config.stopwords_file, 'r', encoding='utf8') as reader:
            for line in reader:
                line = line.strip()
                stop_words.append(line)
        return stop_words

    def _build_param(self):

        def _cal_param(lines):
            f = []  # 列表的每一个元素是一个dict，dict存储着一个文档中每个词的出现次数
            df = {}  # 存储每个词及出现了该词的文档数量
            idf = {}  # 存储每个词的idf值
            length = len(lines)
            words_count = 0
            docs_list = []
            line_length_list =[]
            for line in lines:
                line = str(line)
                line = line.strip()
                if not line:
                    continue
                words = [word for word in jieba.lcut(line) if word and word not in self._stop_words]
                line_length_list.append(len(words))
                docs_list.append(line)
                words_count += len(words)
                tmp_dict = {}
                for word in words:
                    tmp_dict[word] = tmp_dict.get(word, 0) + 1
                f.append(tmp_dict)
                for word in tmp_dict.keys():
                    df[word] = df.get(word, 0) + 1
            for word, num in df.items():
                idf[word] = math.log(length - num + 0.5) - math.log(num + 0.5)
            param = BM25Param(f, df, idf, length, words_count / length, docs_list, line_length_list)
            return param

        def _read_docs():
            '''提取候选集'''
            df = pd.read_excel(self.config.douyin_search_data_xls_file)
            titles = np.array(df['title']).tolist()
            return titles

        # cal
        if self.config.douyin_search_data_xls_file:
            if not os.path.exists(self.config.douyin_search_data_xls_file):
                raise Exception(f"input docs {self.config.douyin_search_data_xls_file} not found")
            lines = _read_docs()
            param = _cal_param(lines)

        # with open(self.config.bm25_param_file, 'wb') as writer:
        #     pickle.dump(param, writer)
        return param

    # def _load_param(self):
    #     self._stop_words = self._load_stop_words()
    #     if self.config.douyin_search_data_xls_file:
    #         param = self._build_param()
    #     else:
    #         if not os.path.exists(self.config.bm25_param_file):
    #             param = self._build_param()
    #         else:
    #             with open(self.config.bm25_param_file, 'rb') as reader:
    #                 param = pickle.load(reader)
    #     return param

    def _cal_similarity(self, words, index):
        score = 0
        for word in words:
            if word not in self.param.f[index]:
                continue
            molecular = self.param.idf[word] * self.param.f[index][word] * (self.param.k1 + 1)
            denominator = self.param.f[index][word] + self.param.k1 * (1 - self.param.b +
                                                                       self.param.b * self.param.line_length_list[index] /
                                                                       self.param.avg_length)
            score += molecular / denominator
        return score

    def cal_similarity(self, query: str):
        """
        相似度计算，无排序结果
        :param query: 待查询结果
        :return: [(doc, score), ..]
        """
        words = [word for word in jieba.lcut(query) if word and word not in self._stop_words]
        score_list = []
        for index in range(self.param.length):
            score = self._cal_similarity(words, index)
            score_list.append((self.param.docs_list[index], score))
        return score_list

    def cal_similarity_rank(self, query: str):
        """
        相似度计算，排序
        :param query: 待查询结果
        :return: [(doc, score), ..]
        """
        result = self.cal_similarity(query)
        result.sort(key=lambda x: -x[1])
        return result

    def cal_text_similarity(self, query, question):
        '''计算两个文本相似度'''
        queryWords = [word for word in jieba.lcut(query) if word and word not in self._stop_words]
        if question not in self.param.docs_list:
            return 10
        else:
            index = self.param.docs_list.index(question)
        if index:
            score = self._cal_similarity(queryWords, index)
        else:
            score = 0

        return score


if __name__ == '__main__':
    from config import Config
    config = Config("", "", "/Users/mickey/project/对话图谱/douyin_service/result/hot_tracking/2022-11-21/JMZ/hot_tracking/douyin_hot_keywords_tmp", "", "", "", "", "")
    bm25 = BM25(config)
    query = '官方明确高风险区解除标准'
    question = '#铁岭 关于划定银州区、铁岭县、开原市风险区的通告'
    print(bm25.cal_text_similarity(query, question))
    # query_content = "自然语言处理并不是一般地研究自然语言"
    # result = bm25.cal_similarity(query_content)
    # for line, score in result:
    #     print(line, score)
    # print("**"*20)
    # result = bm25.cal_similarity_rank(query_content)
    # for line, score in result:
    #     print(line, score)