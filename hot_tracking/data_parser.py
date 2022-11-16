import pandas as pd
import numpy as np
import json
import re
from .config import Config


class DouyinDataset(object):
    '''
    处理抖音热榜数据
    '''
    def __init__(self, config):
        self.config = config
        self.douyin_hot_top_file = config.douyin_hot_top_file
        self.douyin_hot_video_file = config.douyin_hot_video_file
        self.douyin_data_file = config.douyin_data_file
        # 数据来源、热榜主题、id、标题、创作者、热度值、播放量、点赞量、评论量、转发量、收藏量、类别、标签、关键字
        self.columns = ['source', 'hot_title', 'flag_score', 'item_id', 'title', 'author_name', 'hot_score', 'play_count', 'like_count', 'comment_count', 'share_count', 'save_count', 'category', 'tags', 'key_words']

    def extract_tags(self, text, symbol):
        tags = []
        if symbol in text:
            text = text.replace('\n', ' ')
            locs = [i.start() for i in re.finditer(symbol, text)]
            for loc in locs:
                tag = text[loc:].split(symbol)[1].split(' ')[0].split(symbol)[0]
                tags.append(symbol + tag)

        return tags

    def parser_douyin_hot_top_data(self):
        '''抖音热榜数据处理'''
        jsonData = json.loads(open(self.douyin_hot_top_file, 'r', encoding='utf-8').read())
        records = jsonData['RECORDS']
        contents = []
        for record in records:
            video_info = record['video_info']
            flag_score = record['flag_score']
            if flag_score <= self.config.top_count:
                continue
            # video_info = eval(record['video_info'])
            for video in video_info:
                content = {}
                content['source'] = 'hot_top'
                content['category'] = record['category']
                content['hot_score'] = record['hot_score']
                content['hot_title'] = record['hot_title']
                content['flag_score'] = record['flag_score']

                content['title'] = video['title'].strip()
                content['like_count'] = video['digg_count']
                content['comment_count'] = video['comment_count']
                content['share_count'] = video['share_count']
                content['save_count'] = video['collect_count']
                content['item_id'] = video['item_id']
                content['tags'] = video['topics'].strip(',').split(',')

                atWords = self.extract_tags(video['title'], '@')
                tagWords = self.extract_tags(video['title'], '#')
                words = atWords + tagWords
                for tag in words:
                    content['title'] = content['title'].replace(tag, "").strip()

                contents.append(content)

        print("热榜视频数量：", len(contents))
        return contents


    def parser_douyin_hot_video_data(self):
        '''处理抖音热门视频数据'''
        jsonData = json.load(open(self.douyin_hot_video_file, 'r', encoding='utf-8'))
        records = jsonData['RECORDS']
        contents = []
        for record in records:
            content = {}
            content['source'] = 'hot_video'
            content['title'] = record['title'].strip()
            content['flag_score'] = record.get('flag_score', 1)
            content['item_id'] = record['item_id']
            content['author_name'] = record['author_name']
            content['comment_count'] = record['comment_count']
            content['hot_score'] = record['hot_score']
            content['like_count'] = record['like_count']
            content['play_count'] = record['play_count']
            content['key_words'] = record['key_words']
            content['share_count'] = record['share_count']
            # tags = record['title'].split('#')[1:]
            content['tags'] = record['topics'].strip(',').split(',')

            atWords = self.extract_tags(record['title'], '@')
            tagWords = self.extract_tags(record['title'], '#')
            words = atWords + tagWords
            for tag in words:
                content['title'] = content['title'].replace(tag, "").strip()
            contents.append(content)

        print("热门视频数量：", len(contents))
        return contents

    def merge_dataset(self):
        '''将所有类型数据整合起来'''
        hotTopDataset = self.parser_douyin_hot_top_data()
        # hotVideoDataset = self.parser_douyin_hot_video_data()
        dataset = hotTopDataset #+ hotVideoDataset

        results = []
        for data in dataset:
            result = []
            for col in self.columns:
                value = data.get(col, "")
                result.append(value)

            results.append(result)


        print("总的抖音数据为：", len(results))

        dataFrame = pd.DataFrame(results, columns=self.columns)
        dataFrame.to_excel(self.douyin_data_file, index=False)

        print("抖音数据写入文件成功！")

        return dataFrame






if __name__ == '__main__':
    config = Config()
    douyinDataset = DouyinDataset(config)
    douyinDataset.merge_dataset()