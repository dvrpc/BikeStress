#run thru conda cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\4_NetworkIslandAnalysis.py

import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys
import json
import scipy.spatial
import networkx as nx

from database import connection

#table names
TBL_ALL_LINKS = "links"
TBL_CENTS = "block_centroids"
TBL_LINKS = "tolerablelinks"
TBL_NODES = "nodes"
TBL_TOLNODES = "tol_nodes"
TBL_GEOFF_LOOKUP = "geoffs"
TBL_GEOFF_LOOKUP_GEOM = "geoffs_viageom"
TBL_MASTERLINKS = "master_links"
TBL_MASTERLINKS_GEO = "master_links_geo"
TBL_MASTERLINKS_GROUPS = "master_links_grp"
TBL_GROUPS = "groups"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "tolerableturns"
TBL_BRIDGECENTS = "bridge_buffer_cents"

TBL_NODENOS = "nodenos"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_NODES_GID = "nodes_gid"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_OD = "OandD"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
TBL_GEOFF_GROUP = "geoff_group"
TBL_GID_NODES = "gid_nodes"
TBL_NODE_GID = "node_gid_post"

IDX_GEOFF_GROUP = "geoff_group_value_idx"
IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx"
IDX_NODENOS = "nodeno_idx"
IDX_NODES_GEOFF = "nodes_geoff_idx"
IDX_NODES_GID = "nodes_gid_idx"
IDX_GEOFF_NODES = "geoff_nodes_idx"
IDX_GID_NODES = "gid_nodes_idx"
IDX_OD_value = "od_value_idx"
IDX_NODE_GID = "node_gid_post"

cur = connection.cursor()


class Sponge:
    def __init__(self, *args, **kwds):
        self._args = args
        self._kwds = kwds
        for k, v in kwds.iteritems():
            setattr(self, k, v)
        self.code = 0
        self.groups = {}
        
def Group(groupNo, subgraphs, counter):
    for i, g in enumerate(subgraphs):
        for (fg, tg) in g.edges():
            if (fg, tg) in links:
                links[(fg, tg)].groups[groupNo] = counter
            if (tg, fg) in links:
                links[(tg, fg)].groups[groupNo] = counter
        counter += 1
    return counter

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM "{0}";
    """.format(TBL_MASTERLINKS_GEO)
cur.execute(Q_SelectMasterLinks)
#format master links so networkx can read them
h = ['id','fg','tg','cost']
links = {}
for row in cur.fetchall():
    l = Sponge(**dict(zip(h, row)))
    links[(l.fg, l.tg)] = l

print time.ctime(), "Creating Graph"
#create graph
G = nx.MultiDiGraph()
for l in links.itervalues():
    G.add_edge(l.fg, l.tg)

print time.ctime(), "Find Islands"
counter = 0
counter = Group(0, list(nx.strongly_connected_component_subgraphs(G)), counter)
counter = Group(1, list(nx.weakly_connected_component_subgraphs(G)), counter)
counter = Group(2, list(nx.attracting_component_subgraphs(G)), counter)
counter = Group(3, list(nx.connected_component_subgraphs(G.to_undirected())), counter)

results = []
for l in links.itervalues():
    row = [l.id, l.fg, l.tg]
    for i in xrange(4):
        if i in l.groups:
            row.append(l.groups[i])
        else:
            row.append(None)
    results.append(row)

#get groups back into postgis
Q_CreateLinkGrpTable = """
    CREATE TABLE public."{0}"(
        mixid integer, 
        fromgeoff integer, 
        togeoff integer, 
        strong integer,
        weak integer,
        attracting integer,
        undirected integer
    );""".format(TBL_GROUPS)
cur.execute(Q_CreateLinkGrpTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(results[0]))))
arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)

Q_InsertGrps = """
INSERT INTO
    public."{0}"
VALUES {1}
""".format(TBL_GROUPS, arg_str)

cur.execute(Q_InsertGrps)
connection.commit()
del results

#join strong and weak group number to master links geo
Q_JoinGroupGeo = """
    CREATE TABLE public."{1}" AS(
        SELECT * FROM(
            SELECT 
                t1.*,
                t0.strong,
                t0.weak
            FROM "{0}" AS t0
            LEFT JOIN "{2}" AS t1
            ON t0.mixid = t1.mixid
        ) AS "{1}"
    );""".format(TBL_GROUPS, TBL_MASTERLINKS_GROUPS, TBL_MASTERLINKS_GEO)
cur.execute(Q_JoinGroupGeo)
connection.commit()

# Q_CREATEINDEX = """
# CREATE INDEX montco_master_links_grp_idx
   # ON public.montco_master_links_grp (weak ASC NULLS LAST);
# """


#query to find min and max number of strong
Q_StrongSelect = """
    SELECT strong FROM "{0}"
    WHERE strong IS NOT NULL
    ;""".format(TBL_MASTERLINKS_GROUPS)
cur.execute(Q_StrongSelect)
strong_grps = cur.fetchall()

#print time.ctime(), "Create Group Views"
##iterate over groups
#Q_CreateView = """CREATE VIEW %s AS(
#                    SELECT * FROM "{0}"
#                    WHERE strong = %d)""".format(TBL_MASTERLINKS_GROUPS)
#for grpNo in xrange(0, max(strong_grps)[0]):
#    tblname = "links_grp_%d" % grpNo
#    cur.execute("""DROP VIEW IF EXISTS %s;""" % tblname)
#    #create view for each group
#    cur.execute(Q_CreateView % (tblname, grpNo))

#for level 3 analysis
#Q_CreateView = """CREATE VIEW %s AS(
#    SELECT * FROM "{0}"
#    WHERE strong = %d)
#""".format(TBL_MASTERLINKS_GROUPS)
#for grpNo in xrange(min(strong_grps)[0], (max(strong_grps)[0]+1)):
#    tblname = "links_grp_%d" % grpNo
    #cur.execute("""DROP VIEW IF EXISTS %s;""" % tblname)
    #create view for each group
#    cur.execute(Q_CreateView % (tblname, grpNo))
   