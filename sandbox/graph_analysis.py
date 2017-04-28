# NetworkX/Graph Terminology
# strongly connected components: (i -> j) && (j -> i)
# weakly connected components: (i -> j) || (j -> i)
# attracting components ??

import numpy
import psycopg2 as psql
import networkx as nx

class Sponge:
    def __init__(self, *args, **kwds):
        self._args = args
        self._kwds = kwds
        for k, v in kwds.iteritems():
            setattr(self, k, v)
        self.code = 0
        self.groups = {}

def Group(groupNo, subgraphs, reverse = False):
    condition = {groupNo: 0}
    for i, g in enumerate(subgraphs):
        for (fg, tg) in g.edges():
            if (fg, tg) in links:
                links[(fg, tg)].groups[groupNo] = i
            if reverse and (tg, fg) in links:
                links[(tg, fg)].groups[groupNo] = i
            else:
                condition[groupNo] += 1
    return condition

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

h = ['id','fg','tg','cost']
links = {}
for row in cur.fetchall():
    l = Sponge(**dict(zip(h, row)))
    links[(l.fg, l.tg)] = l

G = nx.MultiDiGraph()
for (fg, tg) in links:
    G.add_edge(fg, tg)
    links[(fg,tg)].code = 1

# condition = {}
# condition.update(Group(0, nx.weakly_connected_component_subgraphs(G)))
# condition.update(Group(1, nx.strongly_connected_component_subgraphs(G)))
# condition.update(Group(2, nx.attracting_component_subgraphs(G)))
# condition.update(Group(3, nx.biconnected_component_subgraphs(G)))

condition = {0:0, 1:0, 10:0, 11:0, 20:0, 21:0, 30:0, 31:0, 32:0}
gs = list(nx.strongly_connected_component_subgraphs(G))
for i, g in enumerate(gs):
    for (fg, tg) in g.edges():
        if (fg, tg) in links:
            links[(fg, tg)].groups[100] = i
            condition[0] += 1
        else:
            condition[1] += 1

gs = list(nx.weakly_connected_component_subgraphs(G))
for i, g in enumerate(gs):
    for (fg, tg) in g.edges():
        if (fg, tg) in links:
            links[(fg, tg)].groups[101] = i
            condition[10] += 1
        else:
            condition[11] += 1

gs = list(nx.attracting_component_subgraphs(G))
for i, g in enumerate(gs):
    for (fg, tg) in g.edges():
        if (fg, tg) in links:
            links[(fg, tg)].groups[102] = i
            condition[20] += 1
        else:
            condition[21] += 1

gs = list(nx.connected_component_subgraphs(G.to_undirected()))
for i, g in enumerate(gs):
    for (fg, tg) in g.edges():
        if (fg, tg) in links:
            links[(fg, tg)].groups[103] = i
            condition[30] += 1
        if (tg, fg) in links:
            links[(tg, fg)].groups[103] = i
            condition[31] += 1
        else:
            condition[32] += 1

results = []
for l in links.itervalues():
    row = [l.id, l.fg, l.tg]
    for i in xrange(100,104):
        if i in l.groups:
            row.append(l.groups[i])
        else:
            row.append(None)
    results.append(row)

cur.execute("""
CREATE TABLE 
    public.montco_links_wtsay
(
    mixid integer,
    fromgeoff integer,
    togeoff integer,
    strong integer,
    weak integer,
    attracting integer,
    undirgrp integer
);
""")
con.commit()
str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(results[0]))))
arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)
cur.execute("""INSERT INTO public.montco_links_wtsay VALUES """ + arg_str)
