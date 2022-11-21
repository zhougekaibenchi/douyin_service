from transformers import BertTokenizer, BertModel, BertConfig
import torch.nn as nn
import torch
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
from tqdm import tqdm
import math
import numpy as np




class RecallByBert(object):
    """
    基于bert语义的召回策略
    """
    def __init__(self, config):
        super(RecallByBert, self).__init__()
        self.config = config

        self.tokenizer = BertTokenizer.from_pretrained(self.config.pretrained_model_path + '/vocab.txt')

        self.bert = BertTextEncoder(self.config.pretrained_model_path, self.config.max_length)


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

    def bert_embedding(self, texts):
        '''使用bert获取句向量'''
        input_ids, token_type_ids, attention_masks = [], [], []
        for text in texts:
            tokens = self.tokenizer.tokenize(text=text)
            if len(tokens) > self.config.max_sequence_length:
                input_id = self.tokenizer.convert_tokens_to_ids(tokens[:self.config.max_sequence_length])
                attention_mask = [1] * len(input_id)
            else:
                input_id = self.tokenizer.convert_tokens_to_ids(tokens) + [0] * (self.config.max_sequence_length - len(tokens))
                attention_mask = [1] * len(tokens) + [0] * (self.config.max_sequence_length - len(tokens))
            token_type_id = [0] * len(input_id)

            input_ids.append(input_id)
            token_type_ids.append(token_type_id)
            attention_masks.append(attention_mask)

        input_ids = torch.tensor(input_ids)
        attention_masks = torch.tensor(attention_masks)
        token_type_ids = torch.tensor(token_type_ids)

        embeddings = self.bert(input_ids, token_type_ids, attention_masks)

        return embeddings.detach().numpy()

    def get_embedding_batch(self, inputs):
        batch_size = self.config.batch_size
        batchs = math.ceil(len(inputs) / batch_size)

        flag = True
        for batch in range(batchs):
            start = batch * batch_size
            end = (batch + 1) * batch_size
            batch_inputs = inputs[start: end]
            batch_embeddings = self.bert_embedding(batch_inputs)
            if flag:
                embedding_sum = batch_embeddings
                flag = False
            else:
                embedding_sum = np.vstack([embedding_sum, batch_embeddings])


        return embedding_sum



    def predict_by_bert(self, input1, input2):
        '''基于BERT召回'''
        input1_embedding = self.bert_embedding(input1)
        input2_embedding = self.bert_embedding(input2)

        score = self.similar_count(input1_embedding.detach().numpy()[0], input2_embedding.detach().numpy()[0])

        return score


    def predict_batch_by_bert(self, input1s, input2s):
        '''基于BERT召回'''
        input1s_embedding = self.get_embedding_batch(input1s)
        input2s_embedding = self.get_embedding_batch(input2s)

        assert len(input1s_embedding) != len(input2s_embedding), '句子数量必须相等'

        scores = []
        for i in len(input1s_embedding):
            score = self.similar_count(input1s_embedding[i], input2s_embedding[i])
            scores.append(score)

        return scores



class BertTextEncoder(nn.Module):
    def __init__(self, pretrained_model_path, hidden_sizes):
        super(BertTextEncoder, self).__init__()

        modelConfig = BertConfig.from_pretrained(pretrained_model_path)
        self.bert = BertModel.from_pretrained(pretrained_model_path, config=modelConfig)

        embedding_dim = self.bert.config.hidden_size

        self.fc1 = nn.Linear(embedding_dim, hidden_sizes[0])
        self.fc2 = nn.Linear(hidden_sizes[0], hidden_sizes[1])
        self.tanh = torch.nn.Tanh()


    def forward(self, input_ids, token_type_ids, attention_mask):
        output = self.bert(input_ids, token_type_ids, attention_mask)
        textEmbeddings = output[0][:, 0, :]
        features = self.fc(textEmbeddings)
        features = self.tanh(features)

        return features




















