import pandas as pd
import distance
from collections import Counter
import copy
from .termsRecognition import TermsRecognition
import jieba
import jieba.posseg as pseg
from .config import Config
import numpy as np


class KeywordMiningByTags(object):
    '''基于热榜视频现有关键字进行挖掘'''
    def __init__(self, config):

        self.config = config

        # jieba分词加载词典
        jieba.load_userdict(self.config.insurance_vocab_file)

        # 加载热榜及热门视频
        self.df = pd.read_excel(config.douyin_data_file)
        self.df['title'] = self.df['title'].astype(str)

        if config.is_stopword:
            stopwords = self.get_stopwords(config.stopwords_file)   # 停用词
            self.df['title'] = self.df['title'].apply(lambda x: ''.join([word for word in jieba.lcut(x) if word not in stopwords]))



    def get_stopwords(self, stopwords_file):
        stopwords = []
        with open(stopwords_file, 'r', encoding='utf-8') as fr:
            for line in fr:
                if line.strip():
                    stopwords.append(line.lower().strip())

        return stopwords


    def calculate_sim(self, text1, text2):
        '''使用编辑距离计算两个句子相似度'''
        return 1 - distance.levenshtein(text1, text2) / max(len(text1), len(text2))


    def get_keytags_from_hot_top(self):
        '''从抖音热榜数据中挖掘关键tag'''
        data = self.df[(self.df['source'] == 'hot_top') & (self.df['category'].isin(self.config.category))]
        tops = data['hot_title'].unique()
        keyTags = []
        tagTops = []

        # 提取每个热榜主题标签频次[top, {tag: count}]
        for top in tops:
            if top == "":
                continue
            tagList = []
            top_data = data[data['hot_title'] == top]
            tags = top_data[top_data['tags'] != "['']"]['tags']
            for tag in tags:
                tagList.extend(eval(tag))

            # 统计每个标签出现的次数
            newTabList = [x.strip() for x in tagList if x.strip()]
            tagFreq = dict(Counter(newTabList))

            # 统计每个标签出现的概率
            tagRate = {}
            for tag, freq in tagFreq.items():
                rate = freq / len(top_data)
                if rate >= self.config.tag_rate:
                    tagRate[tag] = freq / len(top_data)

            # 按照概率值进行排序
            sortTagRate = sorted(tagRate.items(), key=lambda x: x[1], reverse=True)

            # 取前n个tag
            sortTagRate = dict(sortTagRate[:self.config.num_hot_top_tags])


            keyTags.append(sortTagRate)
            tagTops.append(top)

        # 整理所有热榜主题标签，统计每个标签出现频次以及出现热榜主题 {tag: [freq, tops]}
        contents = {}
        for tagTop, tagFreq in zip(tagTops, keyTags):
            for tag, freq in tagFreq.items():
                if contents.get(tag):
                    if tagTop not in contents[tag][1]:
                        contents[tag][1].append(tagTop)
                    contents[tag][0] += freq
                else:
                    contents[tag] = [freq, [tagTop]]

        # 整理成list形式[[tops, tag, freq]]
        hotTags = []
        for tag, (freq, tagTop) in contents.items():
            hotTags.append([tagTop, tag, freq])

        dataFrame = pd.DataFrame(hotTags, columns=['top', 'key', 'score'])
        # 按照频次进行排序
        dataFrame.sort_values(by='score', ascending=False)
        dataFrame.to_excel(self.config.douyin_hot_top_tag_file, index=False)

        print('生成抖音热榜关键tag')
        return dataFrame


    def get_keytags_from_hot_video(self):
        '''从热门视频中挖掘关键tag'''
        data = self.df[self.df['source'] == 'hot_video']
        tags = data['tags']
        tagList = []
        for tag in tags:
            tagList.extend(eval(tag))
        tagFreq = dict(Counter(tagList).most_common(self.config.num_hot_video_tags))
        if tagFreq.get(" "):
            del tagFreq[" "]

        hot_video_tags = []
        for tag, freq in tagFreq.items():
            hot_video_tags.append(['热门视频', tag, freq])

        dataFrame = pd.DataFrame(hot_video_tags, columns=['top', 'key', 'score'])
        dataFrame.to_excel(self.config.douyin_hot_video_tag_file, index=False)

        print('生成抖音热门视频关键tag')
        return dataFrame



    def get_keywords_from_hot_top(self, sortKey):
        '''从热榜数据中挖掘关键词'''
        data = self.df[(self.df['source'] == 'hot_top') & (self.df['category'].isin(self.config.category))]
        tops = data['hot_title'].unique()
        contents = []
        for i, top in enumerate(tops):
            print(i, top)
            top_data = data[(data['hot_title'] == top) & (data['title'] != '')]
            titles = top_data[top_data['title'].replace(' ', '').replace('nan', '') != '']['title'].astype(str)

            # 计算每个热榜主题的自由度、凝聚度、idf等
            generator = TermsRecognition(content=titles, tfreq=1, is_jieba=False, topK=self.config.num_hot_top_keywords, mode=[1, 2])  # 文字版'
            result_dataframe = generator.generate_word(sortKey=sortKey)
            result_dataframe['top'] = top
            contents.append(result_dataframe)


        result = pd.concat(contents)   # 获取所有热榜关键短语
        result = result.applymap((lambda x: "".join(x.split()) if type(x) is str else x))   # 去除Key中的所有空格
        result = result.loc[result['key'].str.len() > 1]   # 删除1个字符的行
        result = result.loc[result['freq'] > 1]  # 过滤掉频率为1的关键短语
        result.sort_values(by=sortKey, ascending=False)
        result.to_excel(self.config.douyin_hot_top_keyword_file, index=False)

        print('生成抖音热榜关键短语')

        return result[['top', 'key', 'score']]


    def get_keywords_from_hot_video(self, sortKey):
        '''从热门视频中挖掘关键词'''
        data = self.df[self.df['source'] == 'hot_video']
        titles = data['title'].astype(str)
        # 计算每个热榜主题的自由度、凝聚度、idf等
        generator = TermsRecognition(content=titles, tfreq=1, is_jieba=False, topK=self.config.num_hot_video_keywords, mode=[1])  # 文字版'
        result_dataframe = generator.generate_word(sortKey)


        result = result_dataframe.applymap((lambda x: "".join(x.split()) if type(x) is str else x))   # 去除Key中的所有空格
        result = result.loc[result['key'].str.len() > 1]   # 删除1个字符的行
        result = result.loc[result['freq'] > 1]  # 过滤掉频率为1的关键短语
        result.sort_values(by=sortKey, ascending=False)
        result.to_excel(self.config.douyin_hot_video_keyword_file, index=False)

        print('生成抖音热门视频关键短语')

        return result[['top', 'key', 'score']]


    def filter_by_sim(self, keyTagAndWordsList):
        '''根据相似度，过滤相似关键词'''
        newKeyTagAndWordsList = copy.copy(keyTagAndWordsList)
        for i in range(len(keyTagAndWordsList) - 1):
            for j in range(i+1, len(keyTagAndWordsList)):
                if self.calculate_sim(keyTagAndWordsList[i][1], keyTagAndWordsList[j][1]) > 0.6:
                    if keyTagAndWordsList[j] in newKeyTagAndWordsList:
                        newKeyTagAndWordsList.remove(keyTagAndWordsList[j])

        return newKeyTagAndWordsList

    def filter_by_posseg(self, keyTagAndWordsList):
        '''根据词性过滤关键短语'''
        newKeyTagAndWordsList = []

        for keyTagAndWords in keyTagAndWordsList:
            # 删除不包含名词的短句
            textPosseg = [token.flag for token in pseg.lcut(keyTagAndWords[1])]

            if len(textPosseg) == 1 and textPosseg[0] in ['ns']:
                continue

            for textPos in textPosseg:
                if 'n' in textPos:
                    newKeyTagAndWordsList.append(keyTagAndWords)
                    break

        return newKeyTagAndWordsList

    def filter_by_length(self, keyTagAndWordsList):
        '''根据长度进行过滤'''
        newKeyTagAndWordsList = []
        for keyTagAndWords in keyTagAndWordsList:
            if keyTagAndWords not in newKeyTagAndWordsList and len(keyTagAndWords[1]) < 8:
                newKeyTagAndWordsList.append(keyTagAndWords)

        return newKeyTagAndWordsList

    def filter_by_contain(self, keyTagAndWordsList):
        '''过滤包含在其他短语中的关键词，比如：二十大、二十大报告'''
        newKeyTagAndWordsList = copy.copy(keyTagAndWordsList)
        for i in range(len(keyTagAndWordsList) - 1):
            for j in range(i+1, len(keyTagAndWordsList)):
                if keyTagAndWordsList[i][1] in keyTagAndWordsList[j][1] and keyTagAndWordsList[i] in newKeyTagAndWordsList:
                    newKeyTagAndWordsList.remove(keyTagAndWordsList[i])

        return newKeyTagAndWordsList


    def merge_nr_or_ns_entity(self, keyTagAndWordsList):
        '''过滤包含在其他短语中的关键词，比如：二十大、二十大报告'''
        newKeyTagAndWordsList = []

        for keyTagAndWords in keyTagAndWordsList:
            # 删除不包含名词的短句
            if isinstance(keyTagAndWords[0], list):
                top = keyTagAndWords[0][0]
            else:
                top = keyTagAndWords[0]
            textPosseg = [(token.flag, token.word) for token in pseg.lcut(top)]

            entities = []
            textPoses = []

            for textPos, word in textPosseg:
                if textPos in ['ns', 'nr', 'nrfg', 'nrt']:
                    entities.append(word)
                    textPoses.append(textPos)

            # 主题中包含地名和人名，则在检索词前面添加人名或地名主体
            if len(entities) == 1:
                if entities[0] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[0] + keyTagAndWords[1]

            # 主题中同时包含地名和机构名，则检索词前面添加地名+机构名
            elif 'ns' in textPoses and 'nt' in textPoses:
                if entities[textPoses.index('nt')] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[textPoses.index('nt')] + keyTagAndWords[1]
                if entities[textPoses.index('ns')] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[textPoses.index('ns')] + keyTagAndWords[1]


            # 主题中同时包含地名和人名，则检索词前面添加地名+人名
            elif 'ns' in textPoses:
                if 'nr' in textPoses and entities[textPoses.index('nr')] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[textPoses.index('ns')] + entities[textPoses.index('nr')] + keyTagAndWords[1]
                elif 'nrfg' in textPoses and entities[textPoses.index('nrfg')] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[textPoses.index('ns')] + entities[textPoses.index('nrfg')] + keyTagAndWords[1]
                elif 'nrt' in textPoses and entities[textPoses.index('nrt')] not in keyTagAndWords[1]:
                    keyTagAndWords[1] = entities[textPoses.index('ns')] + entities[textPoses.index('nrt')] + keyTagAndWords[1]


            newKeyTagAndWordsList.append(keyTagAndWords)

        return newKeyTagAndWordsList


    def merge_keywords(self):
        '''将多路结果进行merge得到最终的结果'''

        hot_top_tags, hot_top_keywords, hot_video_tags, hot_video_keywords = pd.DataFrame([]), pd.DataFrame([]), pd.DataFrame([]), pd.DataFrame([])

        if self.config.is_hot_top_tags:
            hot_top_tags = self.get_keytags_from_hot_top()
        if self.config.is_hot_top_keywords:
            hot_top_keywords = self.get_keywords_from_hot_top(sortKey="score")
        if self.config.is_hot_video_tags:
            hot_video_tags = self.get_keytags_from_hot_video()
        if self.config.is_hot_video_keywords:
            hot_video_keywords = self.get_keywords_from_hot_video(sortKey="score")

        # 将多路召回结果进行拼接
        keyTagAndWords = pd.concat([hot_top_tags, hot_top_keywords, hot_video_tags, hot_video_keywords])

        # 将搜索词转换成大写
        keyTagAndWords['key'] = keyTagAndWords['key'].str.upper()
        keyTagAndWords['len'] = keyTagAndWords['key'].apply(lambda x: len(x))

        # 按照长度进行排序
        keyTagAndWords.sort_values(by='len', ascending=True, inplace=True)


        keyTagAndWordsList = np.array(keyTagAndWords).tolist()

        # 根据词性过滤关键短语
        keyTagAndWordsList = self.filter_by_posseg(keyTagAndWordsList)

        # 基于语义相似度过滤重复内容
        keyTagAndWordsList = self.filter_by_sim(keyTagAndWordsList)

        # 根据长度过滤关键短语
        keyTagAndWordsList = self.filter_by_length(keyTagAndWordsList)

        # 过滤包含关键词
        keyTagAndWordsList = self.filter_by_contain(keyTagAndWordsList)

        # 若主题中包含人名或地名，则添加到检索词中
        keyTagAndWordsList = self.merge_nr_or_ns_entity(keyTagAndWordsList)


        dataFrame = pd.DataFrame(keyTagAndWordsList, columns=['top', 'keyword', 'score', 'len'])
        dataFrame.dropna(subset=['keyword'], inplace=True)
        dataFrame.drop(['len'], axis=1, inplace=True)
        dataFrame.to_excel(self.config.douyin_hot_tag_keyword_file, index=False)

        print("获取最终关键标签及短语完成！")

        return dataFrame



if __name__ == '__main__':
    config = Config()

    keywordMining = KeywordMiningByTags(config)

    keywordMining.merge_keywords()    # 是否使用热门视频关键短语数据


