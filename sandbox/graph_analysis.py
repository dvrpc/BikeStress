# NetworkX/Graph Terminology
# strongly connected components: (i -> j) && (j -> i)
# weakly connected components: (i -> j) || (j -> i)
# attracting components ??

import numpy
import psycopg2 as psql
import networkx as nx

con = psql.connect(
    dbname = "BikeStress",
    host = "yoshi",
    port = 5432,
    user = "postgres", 
    password = "sergt"
)
cur = con.cursor()
cur.execute("""
SELECT
    mixid,
    fromgeoff,
    togeoff,
    cost
FROM "montco_master_links_geo";
""")
h = ['id','tg','fg','cost']
# links = map(lambda row:dict(zip(h, row)), cur.fetchall())
links = cur.fetchall()

fgtg_id = {}
fgtg_code = {}
G = nx.MultiDiGraph()
for id, fg, tg, cost in links:
    fgtg_id[(fg, tg)] = id
    fgtg_code[(fg, tg)] = 0
    G.add_edge(fg, tg, key = id, weight = cost)

print nx.is_strongly_connected(G), nx.number_strongly_connected_components(G)
print nx.is_weakly_connected(G), nx.number_weakly_connected_components(G)
print nx.is_attracting_component(G), nx.number_attracting_components(G)
print nx.is_semiconnected(G)
#...wut

if nx.is_semiconnected(G):
    gs = nx.attracting_component_subgraphs(G)
else:
    G = nx.MultiGraph()
    for id, fg, tg, cost in links:
        G.add_edge(fg, tg, key = id, weight = cost)
    gs = nx.connected_component_subgraphs(G)

fgtg_magic = {}
result = []
for i, g in enumerate(gs):
    for (fg, tg) in g.edges():
        if (fg, tg) in fgtg_id:
            id = fgtg_id[(fg, tg)]
            fgtg_code[(fg, tg)] = 1
            result.append((id, fg, tg, i))
        else:
            fgtg_magic[(fg, tg)] = None

cur.execute("""
CREATE TABLE 
    public.montco_links_wtsay
(
    mixid integer,
    fromgeoff integer,
    togeoff integer,
    group intger
);
""")

str_rpl = ",".join("%s" for _ in xrange(len(result[0])))
arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)
cur.execute("""
INSERT INTO
    public.montco_links_wtsay
VALUES """ + args_str)
