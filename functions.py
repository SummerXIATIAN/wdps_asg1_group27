# -*- coding: utf-8 -*-
# @Time : 2021/11/18 4:47 上午
# @Author : Ruin
# @Email : wangkairui0108@gmail.com
# @File : functions.py
# @software : PyCharm
import urllib3
import re

import nltk
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import numpy as np
from elasticsearch import Elasticsearch
import json
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import WordNetLemmatizer


# get the HTML code
def get_HTML(payload, key):
    html = ''
    flag = 0
    for line in payload.splitlines():
        if line.startswith(key):
            flag = 1
        if flag == 1:
            html += line
    return html


# strip the tags in html
def strip_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    soup.prettify()
    [s.extract() for s in soup(['iframe', 'script', 'style'])]
    stripped_text = soup.get_text()
    stripped_text = re.sub(r'[\r|\n|\r\n]+', '\n', stripped_text)
    return stripped_text


# extract the candidates and description of them
def search(query):
    e = Elasticsearch()
    p = {"query": {"query_string": {"query": query}}}
    response = e.search(index="wikidata_en", body=json.dumps(p))
    id_labels = {}
    descriptions = []
    if response:
        for hit in response['hits']['hits']:
            try:
                description = hit['_source']['schema_description']
            except:
                description = None
                pass
            descriptions.append(description)
            label = hit['_source']['schema_name']
            id = hit['_id']
            id_labels.setdefault(id, set()).add(label)

    return id_labels, descriptions


# use nltk preprocess the input text
def preprocess(str):
    # split to sentence
    sentences = nltk.sent_tokenize(str)

    # split to tokens
    # drop the tokens which length>15
    # remove the punctuations
    punctuations = [',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$', '%']
    token = []
    for sen in sentences:
        words = word_tokenize(sen)
        if len(words) > 5:
            words = [word.lower() for word in words if word not in punctuations]
            token.append(words)
    for j in range(len(token)):
        for i in token[j].copy():
            if len(i) > 15:
                token[j].remove(i)

    # filter the stop words
    # what about date?
    stop_words = set(stopwords.words("english"))
    filtered_token = []
    for i in range(len(token)):
        sub_token = []
        for word in token[i]:
            if word.casefold() not in stop_words:
                sub_token.append(word)
        filtered_token.append(sub_token)

    # stem
    stemmer = PorterStemmer()
    stemmed_token = []
    for i in range(len(filtered_token)):
        stemmed_token.append([stemmer.stem(word1) for word1 in filtered_token[i]])
    return filtered_token


# find the sentence that include the entity in the processed text
def get_entity_sentence(entity, cp_filtered_token):
    target_sen = ''
    for sentence in cp_filtered_token:
        try:
            index = sentence.index(entity.lower())
        except:
            index = int(-1)
        if int(index) >= int(0):
            target_sen = sentence.copy()
            print("taget sentence : %s" % target_sen)
            sentence[index] = '_'
            break
    return target_sen


# compare the sentence that include entity with different candidate description
def comparison(candidates, descriptions, entity_sentence):  # input candidates, return the best match one
    max_similarity = 0
    best_entity = None

    count = 0
    for e in candidates:
        description = descriptions[count]
        # 取包含当前entity的sentence
        # 找到后把entity转换为'_'
        #
        #
        # 两个句子的stem后�
        if description == None:
            continue
        else:
            token_description = word_tokenize(description)
            stemmer = PorterStemmer()
            stemmed_description = []
            for tokens in token_description:
                tokens = stemmer.stem(tokens)
                stemmed_description.append(tokens)

            all_tokens = []
            stemmed_sentence = []
            for tokens in entity_sentence:
                for token in tokens:
                    all_tokens.append(token)
            for token in all_tokens:
                tokens = stemmer.stem(token)
                stemmed_sentence.append(tokens)
            # build BOW
            str1 = ''
            for token in stemmed_sentence:
                str1 = str1 + " " + token
            str2 = ''
            for token in stemmed_description:
                str2 = str2 + " " + token
            corpus = {str1, str2}
            vectorizer = CountVectorizer()
            X = vectorizer.fit_transform(corpus)

            A = X.toarray()[0]
            B = X.toarray()[1]
            vec_a = np.mat(A)
            vec_b = np.mat(B)
            num = float(vec_a * vec_b.T)
            denom = np.linalg.norm(vec_a) * np.linalg.norm(vec_b)
            similarity = num / denom
            if similarity > max_similarity:
                max_similarity = similarity
                best_entity = e
        count += 1
    return best_entity
