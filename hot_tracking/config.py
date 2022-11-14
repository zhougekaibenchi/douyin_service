import os

class Config(object):
    def __init__(self, hotvideo_path, hotkeywords_path, hotkeywords_tmp_path, searchvideo_path, recallvideo_path, hottracking_result_path):

        # 判断目录文件是否存在， 不存在则创建
        if not os.path.exists(hotvideo_path):
            os.makedirs(hotvideo_path)

        if not os.path.exists(hotkeywords_path):
            os.makedirs(hotkeywords_path)

        if not os.path.exists(hotkeywords_tmp_path):
            os.makedirs(hotkeywords_tmp_path)

        if not os.path.exists(searchvideo_path):
            os.makedirs(searchvideo_path)

        if not os.path.exists(recallvideo_path):
            os.makedirs(recallvideo_path)

        if not os.path.exists(hottracking_result_path):
            os.makedirs(hottracking_result_path)

        # 热榜数据
        self.douyin_hot_top_file = hotvideo_path + "/douyin_hot_top.json"     # 抖音热榜数据
        self.douyin_hot_video_file = hotvideo_path + "/douyin_open_hot_video.json"    # 抖音热门数据

        # 热门短语数据
        self.douyin_hot_tag_keyword_file = hotkeywords_path + '/douyin_hot_tag_keywords.xls'  # 最终输入热点关键标签或短语

        # 热门短语中间文件
        self.douyin_data_file = hotkeywords_tmp_path + "/douyin_dataset.xls"  # 抖音热榜及热门整理后的数据
        self.douyin_hot_top_tag_file = hotkeywords_tmp_path + '/douyin_hot_top_keytops.xls'  # 抖音热榜关键主题
        self.douyin_hot_video_tag_file = hotkeywords_tmp_path + '/douyin_hot_video_keytops.xls'  # 抖音热门是哦关键主题
        self.douyin_hot_top_keyword_file = hotkeywords_tmp_path + '/douyin_hot_top_keywords.xls'  # 抖音热榜关键短语
        self.douyin_hot_video_keyword_file = hotkeywords_tmp_path + '/douyin_hot_video_keywords.xls'  # 抖音热门视频关键短语

        # 检索词视频数据
        self.douyin_search_data_json_file = searchvideo_path + '/deal_douyin_search.json'    # 抖音搜索视频数据

        # 检索词视频中间文件
        self.douyin_search_data_xls_file = hotkeywords_tmp_path + '/douyin_search_data.xls'    # 整理后的抖音搜索视频数据
        self.douyin_search_video_content_file = './JMZ/hot_tracking/video_content/'    # 抖音召回视频内容文案数据路径

        # 召回视频数据
        self.douyin_search_hot_video_file = recallvideo_path + '/result_hot_insurance_data.xls'  # 最终挖掘热点视频

        # 最终输入数据
        self.douyin_output_file = hottracking_result_path + '/result_output_file.xls'   # 抖音最终输出文件（包含视频文案）


        # 其他辅助文件
        self.stopwords_file = './hot_tracking/data/stopwords.txt'      # 停用词
        self.insurance_vocab_file = './hot_tracking/data/insurance_vocab.txt'     # 保险相关词向量


        # 热门关键短语挖掘相关配置
        self.category = ['话题互动', '社会', '交通', '文化教育', '时政', '科技', '财经']    # 提取关键短语热榜主题分类
        self.is_stopword = True  # 是否过滤停用词
        self.is_hot_top_tags = True,  # 是否选用热榜标签数据
        self.is_hot_top_keywords = True,  # 是否使用热榜关键短语数据
        self.is_hot_video_tags = False,  # 是否使用热门视频标签数据
        self.is_hot_video_keywords = False   # 是否使用热门视频关键短语

        self.num_hot_top_tags = 5   #  每个热榜主题提取多少个标签信息
        self.num_hot_top_keywords = 5  # 每个热榜主题提取多少个关键短语信息
        self.num_hot_video_tags = 5  # 热门视频提取多少个标签信息
        self.num_hot_video_keywords = 10  # 热门视频提取多少个关键短语信息
        self.tag_rate = 0.3



        # 搜索相关热门视频配置
        self.insurance_topics = ['时政社会社会新闻', '人文社科人文艺术', '时政社会时政新闻', '财经财经知识',
                            '财经投资理财', '财经房产', '医疗健康健康', '医疗健康医疗']








