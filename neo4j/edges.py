import json
import requests
from tqdm import tqdm

url = "http://localhost:7474/db/data/cypher"
in_file = "edges.txt"

idmap = {}
for line in open("idmap.txt"):
    items = line.strip().split("\t")
    idmap[items[0]] = items[1]

pbar = tqdm(total=24430785)

rels = []

for line in open(in_file):
    pbar.update(1)
    items = line.strip().split('\t')
    if len(items) != 3: continue
    tag = items[1]
    if items[0] not in idmap or items[2] not in idmap: continue
    rels.append((idmap[items[0]], tag, idmap[items[2]]))
    if len(rels) >= 20:
        matches = []
        merges = []
        for j in range(len(rels)):
            matches.append("match(i%d) where ID(i%d)=%s match(d%d) where ID(d%d)=%s" % (j,j,rels[j][0],j,j,rels[j][2]))
            merges.append("merge (i%d)-[:%s]->(d%d)" % (j, rels[j][1], j))
        query = " ".join(matches) + " " + " ".join(merges)
        q = {'query': query, 'params':{}}
        qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
        ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
        rels = []

if len(rels) >= 100:
    matches = []
    merges = []
    for j in range(len(rels)):
        matches.append("match(i%d) where ID(i%d)=%s match(d%d) where ID(d%d)=%s" % (j,j,rels[j][0],j,j,rels[j][2]))
        merges.append("merge (i%d)-[:%s]->(d%d)" % (j, rels[j][1], j))
    query = " ".join(matches) + " " + " ".join(merges)
    q = {'query': query, 'params':{}}
    qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
    ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
    rels = []

pbar.close()
