import json
import requests
from tqdm import tqdm

url = "http://localhost:7474/db/data/cypher"
in_file = "edges1.txt"

idmap = {}
for line in open("idmap.txt"):
    items = line.strip().split("\t")
    idmap[items[0]] = items[1]

pbar = tqdm(total=24430785)

for line in open(in_file):
    pbar.update(1)
    items = line.strip().split('\t')
    if len(items) != 3: continue
    tag = items[1]

    query = "MATCH(i) where ID(i)=%s match(d) where ID(d)=%s MERGE (i)-[:%s]->(d)" % (idmap[items[0]], idmap[items[2], tag])
    q = {'query': query, 'params':{}}
    qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
    ret = requests.post(url, data=qstr, auth=("neo4j", "123"))

pbar.close()
