# NetworkX/Graph Terminology
# strongly connected components: (i -> j) && (j -> i)
# weakly connected components: (i -> j) || (j -> i)
# attracting components ??

import numpy
import psycopg2 as psql
import networkx as nx

SQL_GetNetwork = """
SELECT
    mixid,
    fromgeoff,
    togeoff,
    cost
FROM "montco_master_links_geo";
"""

SQL_CreateGroupTable = """
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
"""

class Sponge:
    def __init__(self, *args, **kwds):
        self._args = args
        self._kwds = kwds
        for k, v in kwds.iteritems():
            setattr(self, k, v)
        self.code = 0
        self.groups = {}

class NetX:
    def __init__(self, linkData = {}):
        self.G = nx.MultiDiGraph()
        self._links = linkData
        self._nanalysis = 0
    def Group(self, groupNo, subgraphs, reverse = False):
        for i, g in enumerate(subgraphs):
            for (fg, tg) in g.edges():
                if (fg, tg) in self._links:
                    self._links[(fg, tg)].groups[groupNo] = i
                if reverse and (tg, fg) in self._links:
                    self._links[(tg, fg)].groups[groupNo] = i
    def AnalyzeWeakly(self, groupNo = 1):
        self.Group(
            groupNo,
            list(nx.weakly_connected_component_subgraphs(G))
        )
    def AnalyzeStrongly(self, groupNo = 2):
        self.Group(
            groupNo,
            list(nx.strongly_connected_component_subgraphs(G))
        )
    def AnalyzeAttracting(self, groupNo = 3):
        self.Group(
            groupNo,
            list(nx.attracting_component_subgraphs(G))
        )
    def AnalyzeUndirected(self, groupNo = 4):
        self.Group(
            groupNo,
            list(nx.connected_component_subgraphs(G.to_undirected()))
        )
    def AnalyzeAll(self, groupNos = [1,2,3,4]):
        self.AnalyzeWeakly()
        self.AnalyzeStrongly()
        self.AnalyzeAttracting()
        self.AnalyzeUndirected()
    def GetTable(self):
        tbl = []
        for l in self._links.itervalues():
            row = [l.id, l.fg, l.tg]
            for i in xrange(self._nanalysis):
                if i in l.groups:
                    row.append(l.groups[i])
                else:
                    row.append(None)
            tbl.append(row)
        return sorted(tbl, key = lambda r:r[0])

def Group(groupNo, subgraphs, reverse = False):
    condition = {0:0, 1:0, 2:0}
    for i, g in enumerate(subgraphs):
        for (fg, tg) in g.edges():
            if (fg, tg) in links:
                links[(fg, tg)].groups[groupNo] = i
                condition[0] += 1
            if reverse and (tg, fg) in links:
                links[(tg, fg)].groups[groupNo] = i
                condition[1] += 1
    return condition

def GetNetwork(con):
    cur = con.cursor()
    cur.execute(SQL_GetNetwork)
    h = ['id','fg','tg','cost']
    links = {}
    for row in cur.fetchall():
        l = Sponge(**dict(zip(h, row)))
        links[(l.fg, l.tg)] = l
    return links

def CreateGraph():
    G = nx.MultiDiGraph()
    for (fg, tg) in links:
        G.add_edge(fg, tg)
        links[(fg,tg)].code = 1

def main():
    con = psql.connect(
        dbname = "BikeStress",
        host = "localhost",
        port = 5432,
        user = "postgres", 
        password = "sergt"
    )

    N = NetX(GetNetwork(con))
    N.AnalyzeAll()

    cur = con.cursor()
    cur.execute(SQL_CreateGroupTable)
    con.commit()
    str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(results[0]))))
    arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)
    cur.execute("""INSERT INTO public.montco_links_wtsay VALUES """ + arg_str)
    con.commit()

if __name__ == "__main__":
    main()