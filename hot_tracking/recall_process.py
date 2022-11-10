import json
import datetime
import pandas as pd
from .config import Config
import re
import jieba
import jieba.posseg as pseg
import os



class RecallSearchDataset(object):
    '''从检索抖音数据中召回保险相关内容'''
    def __init__(self, config):

        self.config = config

        jieba.load_userdict(self.config.insurance_vocab_file)

        # 保险词典
        self.insurances = self.load_insurance_vocab(freq=1)
        # 搜索内容
        self.searchDataset = self.read_search_dataset()



    def load_insurance_vocab(self, freq=20000):
        '''加载保险词典'''
        insurances = []
        with open(self.config.insurance_vocab_file, 'r', encoding='utf-8') as fr:
            for line in fr:
                if line.strip():
                    words = line.strip().split('\t')
                    if len(words) == 1 or (len(words) == 2 and int(words[1]) > freq):
                        insurances.append(words[0])

        print('保险词典数量：', len(insurances))
        return insurances



    def read_search_dataset(self):
        '''加载搜索内容'''

        # 查询词、标题ID、标题、播放量、点赞量、评论量、收藏量、转发量、发布时间、持续时间、主题、视频链接、作者ID、作者名、作者粉丝数
        columns = ['key_word', 'item_id', 'title', 'play_count', 'like_count', 'comment_count', 'collect_count', 'share_count',
                   'createTime', 'duration', 'topics', 'video_url', 'author_user_id', 'author_name', 'author_fans', 'tags']

        # data = open(self.douyin_search_data_json_file, 'r', encoding='utf-8').read()
        # dataList = eval(data.replace("ObjectId(", '').replace(")", ""))
        jsonData = eval(open(self.config.douyin_search_data_json_file, 'r', encoding='utf-8').read())
        dataList = jsonData['RECORDS']

        contents = []
        for dataJson in dataList:
            content = []
            for column in columns:
                if column == 'tags':
                    tags = self.extract_tags(dataJson['title'], '#')
                    content.append(tags)
                elif column == 'duration':
                    ts = dataJson['duration'].split(':')
                    if len(ts) == 2:
                        duration = int(ts[0]) * 60 + int(ts[1])
                    else:
                        duration = 0
                    content.append(duration)
                else:
                    value = dataJson.get(column, "")
                    content.append(value)

            contents.append(content)


        searchDataset = pd.DataFrame(contents, columns=columns)
        # 按照时间过滤，选取最近一个星期的数据
        # searchDataset = self.filter_by_time(searchDataset, '2022-10-10 0:0:0')
        # 按照标签过滤，选取保险标签相关的数据
        searchDataset = searchDataset[searchDataset['topics'].isin(self.config.insurance_topics)]
        searchDataset.to_excel(self.config.douyin_search_data_xls_file, index=False)

        print('搜索视频数量：', len(searchDataset))

        return searchDataset

    def filter_by_time(self, dataFame, target_time):
        '''按照时间过滤数据'''
        dataFame['createTime'] = pd.to_datetime(dataFame['createTime'])
        data = dataFame[dataFame['createTime'] >= pd.to_datetime(target_time)]
        return data


    def extract_tags(self, text, symbol):
        tags = []
        if symbol in text:
            text = text.replace('\n', ' ')
            locs = [i.start() for i in re.finditer(symbol, text)]
            for loc in locs:
                tag = text[loc:].split(symbol)[1].split(' ')[0].split(symbol)[0]
                tags.append(symbol + tag)

        return tags

    def recall_dataset_by_insurances(self):
        '''基于保险关键词进行内容召回'''

        recallDataset = pd.DataFrame([])
        # 若title中包含保险关键词，则进行召回
        for row in self.searchDataset.iterrows():
            poseg = [token.flag for token in pseg.lcut(row[1]['key_word'])]
            # 如果搜索词为地名或人名则不召回
            if poseg == ['ns'] or poseg == ['nr']:
                continue

            # 根据视频时长进行过滤
            duration = row[1]['duration']
            if duration < 15 or duration > 120:
                continue

            # 若标题中不包含保险关键词则不召回
            title = row[1]['title']
            cutTitle = jieba.lcut(title)
            if set(cutTitle) & set(self.insurances):
                row[1]['insurance_key_word'] = list(set(cutTitle) & set(self.insurances))
                recallDataset = recallDataset.append(row[1], ignore_index=True)

        print("召回视频数量：", len(recallDataset))

        return recallDataset


    def rank_dataset_by_count(self, recallDataset):
        '''对召回视频进行排序'''

        recallDataset = recallDataset[recallDataset['play_count'] != '0']
        recallDataset = recallDataset[recallDataset.ne('').all(axis=1)]
        recallDataset = recallDataset.dropna(axis=0, subset=['play_count', 'like_count', 'comment_count', 'collect_count', 'share_count'])
        recallDataset[['play_count', 'like_count', 'comment_count', 'collect_count', 'share_count']] = recallDataset[['play_count', 'like_count', 'comment_count', 'collect_count', 'share_count']].astype(int)

        # 点赞率=点赞量/播放量
        recallDataset['like_rate'] = recallDataset['like_count'] / (recallDataset['play_count'] + 1)
        # 评论率=评论量/播放量
        recallDataset['comment_rate'] = recallDataset['comment_count'] / (recallDataset['play_count'] + 1)
        # 收藏率=收藏量/播放量
        recallDataset['collect_rate'] = recallDataset['collect_count'] / (recallDataset['play_count'] + 1)
        # 转发率=转发量/播放量
        recallDataset['share_rate'] = recallDataset['share_count'] / (recallDataset['play_count'] + 1)

        recallDataset['score'] = recallDataset['like_rate'] + recallDataset['comment_rate'] + recallDataset['collect_rate'] + recallDataset['share_rate']

        # 获取视频发布天数
        recallDataset['days'] = recallDataset['createTime'].apply(func=lambda x: self.diff_time(x))

        # 根据发布时间调整分值
        recallDataset['score'] = recallDataset.apply(self.rank_score_by_time, axis=1)


        rankDataset = recallDataset.sort_values(by='score', ascending=False)

        rankDataset.to_excel(self.config.douyin_search_hot_video_file, index=False)

        print('排序后的视频数量：', len(rankDataset))

        return rankDataset


    def rank_score_by_time(self, df):
        '''根据发布时间调整分值'''
        if df['days'] <= 7:
            df['score'] /= df['days']

        elif df['days'] <= 14:
            df['score'] /= 10

        elif df['days'] <= 21:
            df['score'] /= 12

        elif df['days'] <= 28:
            df['score'] /= 14

        else:
            df['score'] /= 16

        return df['score']


    # def filter_insurance_vocab(self):
    #     '''清洗保险词典关键词'''
    #     contents = []
    #     with open(self.insurance_vocab_file, 'r', encoding='utf-8') as fr:
    #         for line in fr:
    #             if line.strip():
    #                 tokens = line.strip().split('\t')
    #                 if tokens[0] == '墨西哥':
    #                     print('debug')
    #                 poseg = [token.flag for token in pseg.lcut(tokens[0])]
    #                 # 如果搜索词为地名或人名则不召回
    #                 if len(poseg) == 1 and poseg[0] in ['ns', 'nr', 'nrfg', 'nrt']:
    #                     continue
    #                 else:
    #                     print(line)


    def load_video_content(self):
        '''提取视频内容数据'''
        videoContent = {}
        videoFileNames = os.listdir(self.config.douyin_search_video_content_file)
        for videoFileName in videoFileNames:
            file = os.path.join(self.config.douyin_search_video_content_file, videoFileName)
            with open(file, 'r', encoding='utf-8') as fr:
                content = fr.read().strip()
                if content:
                    # 根据中文长度占比进行过滤
                    if not self.filter_english_video_content(content):
                        continue

                    # 根据视频文案长度进行过滤
                    if not self.filter_video_content_by_length(content):
                        continue

                    videoContent[videoFileName.replace(".txt", "")] = content

        return videoContent

    def filter_english_video_content(self, text, rate=0.8):
        """过滤掉视频文案内容为英文的数据"""
        # 提取字符串中的中文内容
        chText = re.sub("[A-Za-z0-9\,\。]", "", text)

        # 若中文占字符串长度的80%，则保存，否则被删除
        if len(chText) / len(text) >= rate:
            return True

        return False

    def filter_video_content_by_length(self, text, minLength=100):
        '''根据长度过滤视频文案内容'''
        if len(text) >= minLength:
            return True

        return False


    def diff_time(self, createTime):
        '''获取发布时间时长'''
        if createTime != "":
            currTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            d1 = datetime.datetime.strptime(createTime, "%Y-%m-%d %H:%M:%S")
            d2 = datetime.datetime.strptime(currTime, "%Y-%m-%d %H:%M:%S")
            days = (d2 - d1).days
            return days

        return 0


    def merge_video_content(self):
        '''将视频内容整合到召回数据中'''
        textContent = pd.read_excel(self.config.douyin_search_hot_video_file)
        videoContent = self.load_video_content()
        textContent['item_id'] = textContent['item_id'].astype(str)
        textContent['content'] = textContent['item_id'].apply(func=lambda x: videoContent.get(x, ""))
        textContent = textContent[textContent['content'] != ""]
        textContent.dropna(subset=['content'], inplace=True)
        textContent.to_excel(self.config.douyin_output_file, index=False)
        print("视频文案过滤后的数量：", len(textContent))
        print('视频内容整合到文本数据中完成！')







if __name__ == '__main__':
    # print(pseg.lcut('农业保险'))
    # # print(jieba.lcut('建立生育支持体系写入二十大报告，专家：支持生育体系，应对人口老龄化（健康时报）'))
    config = Config()


    recall = RecallSearchDataset(config)
    # 召回
    recallDataset = recall.recall_dataset_by_insurances()
    # # 排序
    rankDataset = recall.rank_dataset_by_count(recallDataset)
    # 将视频内容整合到召回排序结果中
    recall.merge_video_content()






