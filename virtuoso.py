import urllib, json, requests

"""
This file demonstrates how to make a simple SPARQL query against the public
WikiData graph endpoint for the WDPS course.

Additional information:
- Simple SPARQL editor: http://fs0.das5.cs.vu.nl:10011/sparql
- Additional information http://fs0.das5.cs.vu.nl:10011/sparql/?help=intro
- Prefixes in the dataset: http://fs0.das5.cs.vu.nl:10011/sparql/?help=nsdecl
"""

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

query = "select distinct ?Concept where {[] a ?Concept} LIMIT 10"
query = """SELECT DISTINCT ?s WHERE {
  ?s ?label "Twitter"@en .
  ?s ?p ?o 
} LIMIT 10"""
data = sparqlQuery(query)
print(data)
a=1