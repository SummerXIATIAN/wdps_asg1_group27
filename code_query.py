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
nlp = en_core_web_md.load()

KEYNAME = "WARC-TREC-ID"
KEYHTML= "<!DOCTYPE html"
cheats = dict((line.split('\t', 2) for line in open('data/sample-labels-cheat.txt').read().splitlines()))
NER_type = ["DATE","TIME","CARDINAL","ORDINAL","QUANTITY","PERCENT","MONEY"]

def wikidata_query(ent):
    def entity_check(ent):
        if len(ent.split(" ")) > 2:
            return ent.lower()
        else:
            return ent
    
    endpoint_url = "https://query.wikidata.org/sparql"
    
    ent = entity_check(ent)
    query = """SELECT DISTINCT ?s WHERE {{
      ?s ?label "{}"@en .
      ?s ?p ?o
    }}""".format(str(ent))

    def get_results(endpoint_url, query):
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        # TODO adjust user agent; see https://w.wiki/CX6
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    results = get_results(endpoint_url, query)
 
    for result in results["results"]["bindings"]:
        if result["s"]["value"].startswith("http://www.wikidata.org/entity/Q"):
            return (result["s"]["value"])
    return

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
    text = " ".join(re.split(r'[\n\t]+', soup.get_text()))
    text = re.sub(r"\s+", " ", text)
    text = re.sub("[^\u4e00-\u9fa5^\s\.\!\:\-\@\#\$\(\)\_\,\;\?^a-z^A-Z^0-9]","",text)
    return text

def label_process(label):
    if len(label.split(" "))>1:
        return label.title()
    return label

def entity_process(entity):
    l = []
    for X,Y in entity:
        if "cancer" in X:
            X = X.lower()
        l.append((X,Y))
    return l

def ner(text):
    doc = nlp(text)
    entity = [(X.text, X.label_) for X in doc.ents if X.label_ not in NER_type]
    entity = list(set(entity))
    entity = entity_process(entity)
    return entity

def entity_linking(entity):
    l = []
    for e,_ in entity:
        if wikidata_query(e):
            l.append((e,wikidata_query(e)))
    return l

def find_labels2(payload):
    if payload == '':
        return

    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
            
    yield key

# The goal of this function process the webpage and returns a list of labels -> entity ID
def find_labels(payload):
    if payload == '':
        return

    # The variable payload contains the source code of a webpage and some additional meta-data.
    # We firt retrieve the ID of the webpage, which is indicated in a line that starts with KEYNAME.
    # The ID is contained in the variable 'key'
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


    # for label, wikidata_id in cheats.items():
    #     if key and (label in payload):
    #         yield key, label, wikidata_id



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
        file_content = fo.readlines()

        for record in split_records(file_content):
            for key in find_labels2(record):
                if key:
                    try:
                        text = html_to_text(record)
                        entity = ner(text)
                        result = entity_linking(entity)
                        for label, wikidata_id in result:
                            print(key + '\t' + label_process(label) + '\t' + f"<{wikidata_id}>")
                    except Exception as e:
                        pass
                        # print(f"Error on {key}, {e}")
