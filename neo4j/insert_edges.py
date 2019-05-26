import json
import requests
from tqdm import tqdm

url = "http://localhost:7474/db/data/cypher"

records = []
def save(key1, tag, key2):
    global records
    n = len(records)
    cql = 'MATCH (n%d {id:"%s"}), (m%d {id:"%s"}) CREATE (n%d)-[:%s]->(m%d)' % (n, key1, n, key2, n, tag, n)
    records.append(cql)
    
    if len(records) >= 100:
        q = {'query': '\n'.join(records), 'params':{}}
        qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
        ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
        records = []

in_file = "edges_uniq.txt"
cnt = 0
pbar = tqdm(total=25097166)
for line in open(in_file):
    pbar.update(1)
    items = line.strip().split('\t')
    if len(items) != 3: continue
    tag = items[1]
    save(items[0], tag, items[1])

if len(records) > 0:
    q = {'query': '\n'.join(records), 'params':{}}
    qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
    ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
    records = []

pbar.close()
