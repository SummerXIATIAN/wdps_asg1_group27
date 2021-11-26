import gzip
import sys
import json
import requests
import re
from bs4 import BeautifulSoup
from pprint import pprint
import unicodedata
import nltk
import numpy as np
from nltk.tokenize.toktok import ToktokTokenizer
from elasticsearch import Elasticsearch
from contractions import contractions_dict


tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
KEYHTML = "<!DOCTYPE html"

import en_core_web_md
from contractions import contractions_dict

def expand_contractions(text, contraction_mapping=contractions_dict):
    contractions_pattern = re.compile('({})'.format(
        '|'.join(contraction_mapping.keys())), flags=re.IGNORECASE | re.DOTALL)

    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = contraction_mapping.get(match)\
            if contraction_mapping.get(match)\
            else contraction_mapping.get(match.lower())
        expanded_contraction = first_char+expanded_contraction[1:]
        return expanded_contraction
    expanded_text = contractions_pattern.sub(expand_match, text)
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text


def strip_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    soup.prettify()
    [s.extract() for s in soup(['iframe', 'script', 'style'])]
    stripped_text = soup.get_text()
    stripped_text = re.sub(r'[\r|\n|\r\n]+', '\n', stripped_text)
    return stripped_text
def remove_special_characters(text, remove_digits=False):
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text

# remove stop words


def remove_stopwords(text, is_lower_case=False, stopwords=stopword_list):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopwords]
    else:
        filtered_tokens = [
            token for token in tokens if token.lower() not in stopwords]
    filtered_text = ' '.join(filtered_tokens)
    return filtered_text

def find_labels(payload):
    spacy_nlp = en_core_web_md.load()
    if payload == '':
       return
    html = ''
    flag = 0
    for line in payload.splitlines():
        if line.startswith(KEYHTML):
            flag = 1
        if flag == 1:
            html += line
    # get text without html
    text = strip_html_tags(html)
    clean = ''
    for sentence in text:
        clean = clean+remove_special_characters(remove_stopwords(expand_contractions(sentence))if sentence == '' else sentence, remove_digits=True)
    doc = spacy_nlp(clean)

    entities_list = []
    for entity in doc.ents:
        entities_list.append(entity.text)
    return entities_list
def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
    yield payload


if __name__ == '__main__':
    import sys
    try:
        _, INPUT = sys.argv
    except Exception as e:
        print('Usage: python starter-code.py INPUT')
        sys.exit(0)

    count = 0
    with gzip.open(INPUT, 'rt', errors='ignore') as fo:
        for record in split_records(fo):
            if count < 10:  # print only first 10 records currently
                count = count + 1
                find_labels(record)











