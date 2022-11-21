import os

class Config(object):
    def __init__(self, hotvideo_path, hotkeywords_path, hotkeywords_tmp_path, searchvideo_path, recallvideo_path, video_content_path, hottracking_result_path, rewriter_path):

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

        if not os.path.exists(video_content_path):
            os.makedirs(video_content_path)

        if not os.path.exists(hottracking_result_path):
            os.makedirs(hottracking_result_path)

        if not os.path.exists(rewriter_path):
            os.makedirs(rewriter_path)

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

        # 召回视频数据
        self.douyin_search_hot_video_file = recallvideo_path + '/result_hot_insurance_data.xls'  # 最终挖掘热点视频
        self.douyin_search_video_content_file = video_content_path    # 视频文案

        # 最终输出数据
        self.douyin_output_file = hottracking_result_path + '/result_output_file.xls'   # 抖音最终输出文件（包含视频文案）

        # 最终需要改写的数据
        self.douyin_rewriter_file = rewriter_path + '/douyin_rewriter_file.xls'    # 抖音需要文案改写的数据（未改写前）
        self.douyin_rewriter_result_file = rewriter_path + '/douyin_rewriter_result_file.xls'   # 改写后的数据


        # 其他辅助文件
        self.stopwords_file = './hot_tracking/data/stopwords.txt'      # 停用词
        self.insurance_vocab_file = './hot_tracking/data/insurance_vocab.txt'     # 保险相关词向量


        # 热门关键短语挖掘相关配置
        self.category = ['话题互动', '社会', '交通', '文化教育', '时政', '科技', '财经']    # 提取关键短语热榜主题分类
        self.is_stopword = True  # 是否过滤停用词
        self.is_hot_top_tags = True  # 是否选用热榜标签数据
        self.is_hot_top_keywords = True  # 是否使用热榜关键短语数据
        self.is_hot_video_tags = False  # 是否使用热门视频标签数据
        self.is_hot_video_keywords = False   # 是否使用热门视频关键短语

        self.num_hot_top_tags = 5   #  每个热榜主题提取多少个标签信息
        self.num_hot_top_keywords = 5  # 每个热榜主题提取多少个关键短语信息
        self.num_hot_video_tags = 5  # 热门视频提取多少个标签信息
        self.num_hot_video_keywords = 10  # 热门视频提取多少个关键短语信息
        self.tag_rate = 0.3    # 热榜数据标签占比
        self.top_count = 1   # 热榜主题出现频次



        # 搜索相关热门视频配置
        self.insurance_topics = ['时政社会社会新闻', '人文社科人文艺术', '时政社会时政新闻', '财经财经知识',
                            '财经投资理财', '财经房产', '医疗健康健康', '医疗健康医疗', '']


        # 改写接口请求
        self.rewriter_url = 'http://124.196.7.35:8001/api/rewrite'
        self.headers = {
            'Content-Type': 'application.json'
        }
        self.rewriter_params = {
            "text": "有社保的朋友别划走，不管你交的是新农合还是居民医保或者是职工医保，耐心看完今天的视频，帮你省个大几万不是问题",
            "seg_mode": 1,
            "strategy": ["xunfei"]
            # "strategy": [ "baidu", "youdao", "deepL", "xunfei", "synonyms", "facebook"]
        }

        # bert召回参数
        self.pretrained_model_path = '/Users/mickey/project/pre-models/bert/bert-base-chinese'
        self.max_sequence_length = 32
        self.sim_threshold = 0.6
        self.batch_size = 64
        self.hidden_sizes = [384, 128]
        self.bert_recall_dataset = hotkeywords_tmp_path + "/bert_recall_dataset.xls"

#







