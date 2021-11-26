import trident
import json
import spacy
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

import urllib, json, requests
HOST = "http://fs0.das5.cs.vu.nl:10011/sparql"

def sparqlQuery(query, format="application/json"):
    resp = requests.get(HOST + "?" + urllib.parse.urlencode({
        "default-graph": "",
        "should-sponge": "soft",
        "query": query,
        "debug": "on",
        "timeout": "",
        "format": format,
        "save": "display",
        "fname": ""
    }))

    return json.loads(resp.content.decode("utf-8"))


# nltk.download('punkt')#
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')

# nltk.download('maxent_ne_chunker')
# nltk.download('words')
# nltk.download('tagsets')

tokenizer = ToktokTokenizer()
stopword_list = nltk.corpus.stopwords.words('english')
KEYPAGE = "WARC-TREC-ID"
KEYHTML = "<!DOCTYPE html"
# elastic search

#KBPATH = 'assets/wikidata-20200203-truthy-uri-tridentdb'

def get_spacy_label_list():
    root_label = ["CARDINAL", "DATE", "EVENT", "FAC", "GPE", "LANGUAGE", "LAW", "LOC"," MONEY"," NORP", "ORDINAL", "ORG", "PERCENT", "PERSON", "PRODUCT", "QUANTITY", "TIME", "WORK_OF_ART"]
def get_trident_information(candidates):
    select_link = ''
    select_label =''
    max  = -1
    for candidate in candidates:  
        entity_link =candidate[0]
        label = candidate[1]
        count  = 0
        query2="select * { ?s ?p ?o} LIMIT 1000".replace("?s", entity_link)
        json_results = sparqlQuery(query2)   
        if json_results!='':
            binding = json_results['results']['bindings']
            for b in binding:
                count = int(count) +1
        if int(max) <= int(count):                   
            max = int(count)
            select_link = entity_link
            select_label = label
    if select_link!='':
        return (select_link,select_label)
    else:
        return ''

e = Elasticsearch(['http://fs0.das5.cs.vu.nl:10010/'])
def search(query):

    try: 
        p = { "from" : 0, "size" :70,"query" : { "query_string" : { "query" : query }}}
        response = e.search(index="wikidata_en", body=json.dumps(p))
    except:
        return {}
    id_labels = {}
    if response:
        for hit in response['hits']['hits']:
            label = hit['_source']['schema_name']
            id = hit['_id']
            id_labels.setdefault(id, set()).add(label)
    return id_labels

# delete html tags

useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
def strip_html_tags(text):
    soup = BeautifulSoup(text, features="lxml")
    soup.prettify()
    # [s.extract() for s in soup(['iframe','script','style', 'code','title','head','footer','header'])]
    # [s.extract() for s in soup.find_all(id = useless_tags)]
    # [s.extract() for s in soup.find_all(name='div',attrs={"class": useless_tags})]
    # stripped_text = soup.get_text()
    # stripped_text = re.sub(r'[\r|\n|\r\n]+', '\n', stripped_text)
    stripped_text =''
    if (soup.body is not None):
        soup = soup.body

        VALID_TAGS = ['div', 'p']
        # Select only relevant tags:
        for tag in soup.findAll('p'):
            if tag.name not in VALID_TAGS:
                tag.replaceWith(tag.renderContents())
        stripped_text = soup.get_text()
    return stripped_text

# expand text-- this script is from website


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
# remove_special_characters


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

# main deal function


def find_labels(payload):
    
    if payload == '':
        return
    # get the page ID
    page = ''
    flag = 0
    for line in payload.splitlines():
        if line.startswith(KEYPAGE):
            page = line.rsplit(":",1)[1].strip()
            break

    #get html text from Warc
    html = ''
    delete  = ''
    flag = 0
    for line in payload.splitlines(True):
        if line.startswith(KEYHTML):
            flag = 1
        if flag != 1:
            delete = delete+line
    html=payload.replace(delete,"")

    # get text without html tags
    text = strip_html_tags(html)
    # # default sentence tokenizer----now is english, we can apply many language later
    default_st = nltk.sent_tokenize
    sentences = default_st(text=text, language='english')

    # clean sentence
    clean_sentences = [remove_special_characters(remove_stopwords(expand_contractions(sentence))if sentence == '' else sentence, remove_digits=True)
                       for sentence in sentences]
    clean_sentences =[re.sub(r'[\r|\n|\r\n]+', ' ', i) for i in clean_sentences]
    document =[]
    paragraph =''
    labels =[]
    links=[]
    res=[]
    for sentence in clean_sentences:
        if len(paragraph) < 1000:
            paragraph = paragraph +' '+ sentence
        else:
            document.append(paragraph)
            paragraph = ''
    if paragraph !='':
        document.append(paragraph)
    spacy_nlp = spacy.load("en_core_web_sm")
    for par in document: 
        par = spacy_nlp(par)
        entity_list = []
        for element in par.ents:
            text = element.text.strip("\n").replace("\n", "").replace("\r", "").strip()
            num = len(text.split(" "))
            if element.label_ not in ["CARDINAL", "DATE", "QUANTITY", "TIME", "ORDINAL", "MONEY", "PERCENT", "QUANTITY"]:
                if int(num)> 3:
                    for t in text.split(" "):
                        e = { "type" : element.label_, "text":t }
                        entity_list.append(e)
                else:
                    entity = { "type" : element.label_, "text":text }
                    entity_list.append(entity)
        if len(entity_list) > 1:
            print(entity_list)
            for word in entity_list:
                candidates =[]
                if (word["text"] != ''):
                    try:     
                        result=search(word["text"])
                    except:
                        print("es no result")
                        continue
                    try:
                        candidates = [(entity,label) for entity, label in result.items()]
                    except:
                        print("no result")
                        continue
                    try:
                        res=get_trident_information(candidates)
                        if res:
                            labels.append(word["text"])
                            links.append(res[0])
                    except:
                        print("trident no result")
                        continue
    for label,wikidata_id in list(zip(labels,links)):
        if label and wikidata_id:
            label = str(label).replace("'","").replace("{","").replace("}","") 
            if page and label and wikidata_id:     
                yield page, label, wikidata_id


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
    with gzip.open(INPUT, 'rt', errors='ignore') as fo:
        for record in split_records(fo):
            for page,label,wikidata_id in find_labels(record):
               print(page + '\t' + label + '\t' + wikidata_id)

