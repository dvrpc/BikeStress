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

def Group(groupNo, subgraphs):
    condition = {groupNo: 0}
    for i, g in enumerate(subgraphs):
        for (id, fg, tg) in g.edges(keys = True):
            if (fg, tg) in links:
                l = links[(fg, tg)]
                j = 1
            elif (tg, fg) in links:
                l = links[(tg, fg)]
                j = -1
            else:
                condition[groupNo] += 1
                continue
            l.groups[groupNo] = i * j
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
for l in links.itervalues():
    G.add_edge(l.fg, l.tg, key = l.id, weight = l.cost)
    l.code = 1

print nx.is_strongly_connected(G), nx.number_strongly_connected_components(G)
print nx.is_weakly_connected(G), nx.number_weakly_connected_components(G)
print nx.is_attracting_component(G), nx.number_attracting_components(G)
print nx.is_semiconnected(G)

condition = {}
condition.update(Group(0, nx.weakly_connected_component_subgraphs(G)))
condition.update(Group(1, nx.strongly_connected_component_subgraphs(G)))
condition.update(Group(2, nx.attracting_component_subgraphs(G)))
# condition.update(Group(3, nx.biconnected_component_subgraphs(G)))

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
