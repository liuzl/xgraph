import requests
url = "http://localhost:7474/db/data/"
ret = requests.get(url, auth=("neo4j", "123"))
print(ret.text)
