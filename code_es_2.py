import gzip

import functions
import spacyTemplate

KEYNAME = "WARC-TREC-ID"
KEYHTML = "<!DOCTYPE html"


def find_labels(payload):
    if payload == '':
        return
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    bbest_entity = ''  # the best match entity
    link = ''  # the link of the best match entity
    if key != None:
        html = functions.get_HTML(payload, KEYHTML)  # get HTML code from payload
        text = functions.strip_html_tags(html)  # strip the tags in HTML code

        filtered_tokens = functions.preprocess(text)  # get the processed token
        entities_list = spacyTemplate.find_labels(payload)  # NER part, get all entities from payload

        for e in entities_list:  # Iterate all entities in payload
            link_list = []
            label_list = []
            try:
                for link, label in functions.search(e)[0].items():  # get candidates and their link
                    link_list.append(link)
                    label_list.append(label)
                descriptions = functions.search(e)[1]  # get descriptions about candidates
                entity_sen = filtered_tokens
                best_entity = functions.comparison(label_list, descriptions, entity_sen)  # compare the sentence that
                # include the entity with all candidates` descriptions
            except:
                continue
            if best_entity != None:
                index = label_list.index(best_entity)
                link = link_list[index]   # get the link of best entity
                best_list = list(best_entity)
                bbest_entity = best_list[0]
                yield key, bbest_entity, link
            else:
                continue



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
                print(key + '\t' + label + '\t' + wikidata_id)
