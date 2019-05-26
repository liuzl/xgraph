import json
import requests
from tqdm import tqdm

url = "http://localhost:7474/db/data/cypher"

records = []
def save(record):
    global records

    if 'TYPE' in record:
        t = ":%s" % ':'.join(record['TYPE'])
    else: return
    attrs = []
    for k, v in record.items():
        if k == 'TYPE': continue
        one = "%s:%s" % (k, json.dumps("\t".join(v), ensure_ascii=False))
        attrs.append(one)
    cql = "(%s {%s})" % (t, ','.join(attrs))
    records.append(cql)
    
    if len(records) >= 100:
        q = {'query': "CREATE " + ','.join(records), 'params':{}}
        qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
        ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
        records = []

in_file = "nodes.txt"
cnt = 0
key = ''
record = None
pbar = tqdm(total=98613641)
for line in open(in_file):
    pbar.update(1)
    items = line.strip().split('\t')
    if len(items) != 3: continue
    tag = items[1]
    if items[0] != key:
        if record is not None:
            save(record)
        key = items[0]
        record = {"id": [key,]}
        cnt += 1
    else:
        if tag in record: record[tag].add(items[2])
        else: record[tag] = set([items[2],])
if record is not None:
    save(record)

if len(records) > 0:
    q = {'query': ','.join(records), 'params':{}}
    qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
    ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
    records = []

pbar.close()
