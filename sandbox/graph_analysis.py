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
gs = nx.attracting_component_subgraphs(G)
result = []
for i, g in enumerate(gs):
    for (fnn, tnn) in g.edges():
        result.append((fnn, tnn, i))
with open("attracting_components.csv", "wb") as io:
    w = csv.writer(io)
    w.writerow(["FromNodeNo","ToNodeNo","Group"])
    w.writerows(result)