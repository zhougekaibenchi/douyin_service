from transformers import BertTokenizer, BertModel, BertConfig
import torch.nn as nn
import torch
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.metrics.pairwise import cosine_similarity



class BertTextEncoding(nn.Module):
    def __init__(self, config):
        super(BertTextEncoding, self).__init__()

        self.maxLength = config.maxLength    # 最大句子长度
        self.pretrain_model_path = config.pretrain_model_path    # 预训练模型路径

        modelConfig = BertConfig.from_pretrained(self.pretrain_model_path)
        self.roberta = BertModel.from_pretrained(self.pretrain_model_path, config=modelConfig)
        embedding_dim = self.roberta.config.hidden_size
        self.fc = nn.Linear(embedding_dim, self.maxLength)
        self.tanh = nn.Tanh()


    def forward(self, input_ids, attention_mask, token_type_ids):
        pass




class RecallByBert(object):
    """
    基于bert语义的召回策略
    """
    def __init__(self, config, searchDataset):
        super(RecallByBert, self).__init__()
        self.config = config

        # 搜索内容
        self.searchDataset = searchDataset


    def similar_count(self, vec1, vec2, mode='cos'):
        """
        计算向量相似度
        :param vec1: 句向量1
        :param vec2: 句向量2
        :param mode: 用欧式距离还是余弦距离（eu: 欧式距离、cos：余弦相似度）
        :return: 返回两个向量的距离得分
        """

        if mode == 'eu':
            return euclidean_distances([vec1, vec2])[0][1]
        elif mode == 'cos':
            return cosine_similarity([vec1, vec2])[0][1]


