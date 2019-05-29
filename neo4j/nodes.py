import json
import requests
from tqdm import tqdm

url = "http://localhost:7474/db/data/cypher"
in_file = "nodes.txt"
out_file = "idmap.txt"
out = open(out_file, "w")
cnt = 0
key = ''
record = None

pbar = tqdm(total=98613641)

records = []
ids = []
allids = set()

def save(record):
    if "id" not in record: return
    if record['id'][0] in allids: return
    allids.add(record['id'][0])
    global records
    global ids
    global out

    i = len(records)
    if 'TYPE' in record:
        t = "n%d:Node:%s" % (i,':'.join(record['TYPE']))
    else: return
    attrs = []
    for k, v in record.items():
        one = "%s:%s" % (k, json.dumps("\t".join(v), ensure_ascii=False))
        attrs.append(one)
    cql = "(%s {%s})" % (t, ','.join(attrs))
    records.append(cql)
    ids.append(record['id'])
    
    if len(records) >= 100:
        nodes = []
        for j in range(len(records)):
            nodes.append("ID(n%d)" % j)
        q = {'query': "CREATE " + ','.join(records) + " RETURN " + ','.join(nodes), 'params':{}}
        qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
        ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
        a = json.loads(ret.text)["data"][0]
        if len(a) == len(ids):
            for k in range(len(a)):
                out.write("%s\t%s\n" % (ids[k][0], a[k]))
        records = []
        ids = []

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
    nodes = []
    for j in range(len(records)):
        nodes.append("ID(n%d)" % j)
    q = {'query': "CREATE " + ','.join(records) + " RETURN " + ','.join(nodes), 'params':{}}
    qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
    ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
    a = json.loads(ret.text)["data"][0]
    if len(a) == len(ids):
        for k in range(len(a)):
            out.write("%s\t%s\n" % (ids[k][0], a[k]))
    records = []
    ids = []

pbar.close()
out.close()
