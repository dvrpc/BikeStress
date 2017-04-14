# NetworkX/Graph Terminology
# strongly connected components: (i -> j) && (j -> i)
# weakly connected components: (i -> j) || (j -> i)
# attracting components ??

import csv
import numpy
import psycopg2 as psql
import networkx as nx

TBL_LINKS = "mercer_tolerablelinks"
TBL_SPATHS = "mercer_shortestpaths"

SQL_SPLINKS = """
SELECT
    tl.gid,
    tl.no,
    tl.fromnodeno,
    tl.tonodeno,
    tl.linklts,
    tbl0.cnt AS cnt
FROM (
    SELECT
        edge,
        COUNT(*) AS cnt
    FROM "{0}"
    GROUP BY edge
) AS tbl0
INNER JOIN "{1}" AS tl
ON tl.gid = tbl0.edge
""".format(TBL_SPATHS, TBL_LINKS)

con = psql.connect(
    dbname = "BikeStress",
    host = "yoshi",
    port = 5432,
    user = "postgres", 
    password = "sergt"
)
cur = con.cursor()
cur.execute(SQL_SPLINKS)
h = ['gid','no','fnn','tnn','lts','cnt']
data = map(lambda row:dict(zip(h, row)), cur.fetchall())

G = nx.MultiDiGraph()
for l in data:
    G.add_edge(l['fnn'], l['tnn'], weight = l['lts'])

print nx.is_strongly_connected(G), nx.number_strongly_connected_components(G)
print nx.is_weakly_connected(G), nx.number_weakly_connected_components(G)
print nx.is_attracting_component(G), nx.number_attracting_components(G)
print nx.is_semiconnected(G)
#...wut

if nx.is_semiconnected(G):
    gs = nx.attracting_component_subgraphs(G)
    resultfile = "attracting_components.csv"
else:
    G = nx.MultiGraph()
    for l in data:
        G.add_edge(l['fnn'], l['tnn'], weight = l['lts'])
    gs = nx.connected_component_subgraphs(G)
    resultfile = "connected_components.csv"

result = []
for i, g in enumerate(gs):
    for (fnn, tnn) in g.edges():
        result.append((fnn, tnn, i))
with open(resultfile, "wb") as io:
    w = csv.writer(io)
    w.writerow(["FromNodeNo","ToNodeNo","Group"])
    w.writerows(result)

#### Debug
link_id = zip(h.GetMulti(Visum.Net.Links, "FromNodeNo"), h.GetMulti(Visum.Net.Links, "ToNodeNo"))
_link_id = dict((k, v) for v, k in enumerate(link_id))
with open(r"C:\Users\wtsay\Documents\connected_components.csv", "rb") as io:
    r = csv.DictReader(io)
    data = [d for d in r]
av = [0 for _ in xrange(len(link_id))]
for d in data:
    key = tuple(map(int, (d["FromNodeNo"], d["ToNodeNo"])))
    if key in _link_id:
        av[_link_id[key]] = int(d["Group"]) + 1
h.SetMulti(Visum.Net.Links, "AddVal3", av)