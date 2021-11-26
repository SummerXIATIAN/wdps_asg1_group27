import gzip
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import gzip
from bs4 import BeautifulSoup
import re
import spacy
from spacy import displacy
from collections import Counter
import en_core_web_md
import difflib
import requests
import json
from elasticsearch import Elasticsearch
nlp = en_core_web_md.load()

KEYNAME = "WARC-TREC-ID"
KEYHTML= "<!DOCTYPE html"
NER_type = ["DATE","TIME","CARDINAL","ORDINAL","QUANTITY","PERCENT","MONEY"] # ruled type list avoid


## format function for output
def label_process(label):
    if len(label.split(" "))>1:
        return label.title()
    return label

## rule format function in NER
def entity_process(entity):
    l = []
    for X,Y in entity:
        if "cancer" in X:
            X = X.lower()
        l.append((X,Y))
    return l

## retrieve the text from HTML pages
## including text cleaning
def html_to_text(record):
    html = ''
    flag = 0
    for line in record.splitlines():
        if line.startswith(KEYHTML):
            flag = 1
        if flag == 1 :
            html += line

    realHTML = html.replace('\n', '<br>')
    soup = BeautifulSoup(realHTML,features="html.parser")
    for script in soup(["script", "style","aside"]):
        script.extract()
    
    ## text cleaning 
    text = " ".join(re.split(r'[\n\t]+', soup.get_text()))
    text = re.sub(r"\s+", " ", text)
    text = re.sub("[^\u4e00-\u9fa5^\s\.\!\:\-\@\#\$\(\)\_\,\;\?^a-z^A-Z^0-9]","",text)
    return text

## NER function using spaCy
def ner(text):
    doc = nlp(text)
    entity = [(X.text, X.label_) for X in doc.ents if X.label_ not in NER_type]
    entity = list(set(entity))
    entity = entity_process(entity)
    return entity

## funtion of entity linking
## link the query result (wikidata url) with each entity
def entity_linking(entity):
    entity_list = []
    for e,_ in entity:
        if es_search(e):
            entity_list.append((e,es_search(e)))
    return entity_list

## function that finds a most similar entity
def get_closest_word(es_query, es_dictionary):
    try:
        wl = difflib.get_close_matches(es_query, list(es_dictionary.keys()))
        return wl[0]
    except:
        return list(es_dictionary.keys())[0]

### function that requests elasticsearch to get the candidate
def es_search(es_query):
    
    def search(query):
        e = Elasticsearch(["http://fs0.das5.cs.vu.nl:10010/"])
        p = { "from" : 0, "size" : 20, "query" : { "query_string" : { "query" : query }}}
        response = e.search(index="wikidata_en", body=json.dumps(p))
        id_labels = {}
        if response:
            for hit in response['hits']['hits']:
                label = hit['_source']['schema_name']
                id = hit['_id']
                id_labels.setdefault(id, set()).add(label)
        return id_labels
    
    d = {}
    try:
        for entity, labels in search(es_query.lower()).items():
            d[list(labels)[0]] = entity
        res = get_closest_word(es_query,d)
        return d[res]
    except Exception as e:
        print(e)
        return d

# The goal of this function process the webpage and returns a list of labels -> entity ID
def find_labels(payload):
    if payload == '':
        return

    # The variable payload contains the source code of a webpage and some additional meta-data.
    # We firt retrieve the ID of the webpage, which is indicated in a line that starts with KEYNAME.
    # The ID is contained in the variable 'key'
    # cheats = dict((line.split('\t', 2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break

    try:
    # Problem 1: The webpage is typically encoded in HTML format.
    # We should get rid of the HTML tags and retrieve the text. How can we do it?
        text = html_to_text(payload)

    # Problem 2: Let's assume that we found a way to retrieve the text from a webpage. How can we recognize the
    # entities in the text?
        entity = ner(text)

    # Problem 3: We now have to disambiguate the entities in the text. For instance, let's assugme that we identified
    # the entity "Michael Jordan". Which entity in Wikidata is the one that is referred to in the text?
        result = entity_linking(entity)

        for label, wikidata_id in result:
            if key and label and wikidata_id:
                yield key, label, wikidata_id
    
    except:
        pass

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
            for key, label, wikidata_id in find_labels(record):
                # print(key + '\t' + label + '\t' + wikidata_id)
                print(key + '\t' + label_process(label) + '\t' + f"{wikidata_id}")
