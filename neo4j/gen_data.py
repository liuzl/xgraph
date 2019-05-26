from tqdm import tqdm

def get_tag(s):
    items = s.strip().split('@')
    raw1 = items[0].strip()
    items = raw1.split(":")
    raw2 = items[-1].strip()
    items = raw2.split("_")
    return items[-1].strip()

in_file = "../../kball/kball.ntriple"
node_file = "nodes.txt"
edge_file = "edges.txt"

node_fd = open(node_file, "w")
edge_fd = open(edge_file, "w")

pbar = tqdm(total=123894931)
old = ""
one = ""
for line in open(in_file, "rb"):
    line = line.strip().decode("gbk", "ignore")
    pbar.update(1)
    items = line.split('\t')
    if len(items) != 3: continue
    tag = get_tag(items[1])
    old = one
    one = "%s\t%s\t%s\n" % (items[0], tag, items[2])
    if one == old: continue

    if len(items[2]) > 2 and items[2][1] == "_":
        edge_fd.write(one)
    else:
        node_fd.write(one)
pbar.close()
node_fd.close()
edge_fd.close()
