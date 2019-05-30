import requests
import json

from flask import Flask, request
app = Flask(__name__)

url = "http://localhost:7474/db/data/cypher"

@app.route("/")
def hello():
    r = request.args.get('query',"")
    cql = ''
    res = ''
    if r != '':
        query = r
        q_type = 0
        if query.endswith('r'):
            q_type = 1
            query = query[:-1]

        querylist = query.split(u"的")
        arg_len = len(querylist)
        cql = 'MATCH (n0:Node {NAME:"%s"}) ' % querylist[0]

        if arg_len == 1:
            cql += "RETURN n0"
        elif arg_len == 2:
            if q_type == 0:
                cql += "RETURN n0.`%s`" % querylist[1]
            else:
                cql += "MATCH (n0)-[:`%s`]->(b) RETURN b" % querylist[1]
        else:
            i = 1
            while i < arg_len - 1:
                last_node = "n%d" % (i-1)
                cur_node = "n%d" % i
                cql += "MATCH (%s)-[:`%s`]->(%s) " % (last_node, querylist[i], cur_node)
                i += 1
            if q_type == 0:
                cql += "RETURN n%d.`%s`" % (i-1, querylist[arg_len-1])
            else:
                cql += "MATCH (n%d)-[:`%s`]->(n%d) RETURN n%d" % (i-1, querylist[arg_len-1], i, i)
        q = {'query': cql, 'params':{}}
        qstr = json.dumps(q, ensure_ascii=False).encode("utf-8")
        ret = requests.post(url, data=qstr, auth=("neo4j", "123"))
        res = ret.text

    pageStr = '''
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head>
<body>
<form method="get">
    查询: <input type="text" name="query" style = "width:400" value="%s"/>
	<input type="submit" value="submit"/>
</form>
<hr />
cypher: %s
<hr />
<xmp>
%s
</xmp>
</body>
</html>
'''

    return pageStr % (r, cql, res)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
