#####RUN PIECE BY PIECE#####

import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle
import sqlite3
from collections import Counter
import json
import scipy.spatial
import networkx as nx



####table names to modify in subsequent runs###
TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco_L3_tolerablelinks"
TBL_SPATHS = "montco_L3_shortestpaths"
TBL_TOLNODES = "montco_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_geoffs"
TBL_GEOFF_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_master_links_grp"
TBL_GROUPS = "montco_groups"
TBL_EDGE = "montco_L3_edgecounts"
TBL_USE = "montco_L3_linkuse"
TBL_TOP = "montco_L3_topLinks"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#repeated from OD Pair List Shortest Paths script to get OandD into memory again
SQL_GetGeoffs = """SELECT geoffid, vianode, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_GEOFF_GEOM)
SQL_GetBlocks = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_CENTS)

def GetCoords(record):
    id, vianode, geojson = record
    return id, vianode, json.loads(geojson)['coordinates']
def ExecFetchSQL(SQL_Stmt):
    cur = con.cursor()
    cur.execute(SQL_Stmt)
    return map(GetCoords, cur.fetchall())

print time.ctime(), "Section 1"
data = ExecFetchSQL(SQL_GetGeoffs)
world_ids, world_vias, world_coords = zip(*data)
node_coords = dict(zip(world_vias, world_coords))
geoff_nodes = dict(zip(world_ids, world_vias))
# Node to Geoff dictionary (a 'random' geoff will be selected for each node)
nodes_geoff = dict(zip(world_vias, world_ids))
geofftree = scipy.spatial.cKDTree(world_coords)
del world_coords, world_vias

print time.ctime(), "Section 2"
data = ExecFetchSQL(SQL_GetBlocks)
results = []
for i, (id, _, coord) in enumerate(data):
    dist, index = geofftree.query(coord)
    geoffid = world_ids[index]
    nodeno = geoff_nodes[geoffid]
    results.append((id, nodeno))
del data, geoff_nodes, world_ids

con = psql.connect(dbname = "BikeStress", host = "toad", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()
    
# Grab the feasible oGID to dGID paths from the shortest paths table
Q_AvailPairs = """SELECT oGID, dGID FROM "{0}" GROUP BY oGID, dGID;""".format(TBL_SPATHS)
cur.execute(Q_AvailPairs)
#save as python variable
avail_gid_pairs = cur.fetchall()

node_gid = {}
gid_node = {}
for GID, nodeno in results:
    if not nodeno in node_gid:
        node_gid[nodeno] = []
    if GID in gid_node:
        print "Warn %d" % GID
    node_gid[nodeno].append(GID)
    gid_node[GID] = nodeno

pair_count = []
for i, (oGID, dGID) in enumerate(avail_gid_pairs):
    onode = gid_node[oGID]
    dnode = gid_node[dGID]
    row = oGID, onode, dGID, dnode, (len(node_gid[onode]) * len(node_gid[dnode]))
    pair_count.append(row)
    
del avail_gid_pairs

#edge count from original table
Q_EdgeCount = """SELECT edge, COUNT(*) FROM "{0}" GROUP BY edge;""".format(TBL_SPATHS)
cur.execute(Q_EdgeCount)
edge_count = cur.fetchall()
#convert to dictionary
edge_count_dict = dict(edge_count)

#read shortest path table into pythom memory
all_paths = []
batch_size = 1000000L
i = 0L
while (i < 2189474044L):
    Q_SelectSP = """SELECT ogid, dgid, edge FROM public."{0}" LIMIT {1} OFFSET {2};""".format(TBL_SPATHS, batch_size, i)
    cur.execute(Q_SelectSP)
    paths = cur.fetchall()
    all_paths.append(paths)
    i += batch_size

# for i in xrange(0, 2189474044L, batch_size):
    # Q_SelectSP = """SELECT ogid, dgid, edge FROM public."{0}" LIMIT {1} OFFSET {2};""".format(TBL_SPATHS, batch_size, i)
    # cur.execute(Q_SelectSP)
    # paths = cur.fetchall()
    # all_paths.append(paths)
    
weight_by_od = {}
# weight_by_od[1,2] = 3
for i, (oGID, oNode, dGID, dNode, cnt) in enumerate(pair_count):
    o = oGID
    d = dGID
    weight_by_od[(o, d)] = cnt
    
for i, (ogid, dgid, edge) in enumerate(paths):
    path_weight = weight_by_od[ogid, dgid]
    if path_weight > 1:
        edge_count_dict[edge] += (path_weight - 1)
       
#convert dictionary back to  list
edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]

#create table with edge and count to join to geometries and view results
Q_CreateEdgeCountTable = """
    CREATE TABLE IF NOT EXISTS public."{0}"
    (
        edge integer,
        count integer
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
""".format(TBL_EDGE)
cur.execute(Q_CreateEdgeCountTable)
con.commit()

#insert value into that edge count table
Q_InsertEdgeCounts = """
    INSERT INTO "{0}" VALUES (%s, %s);
""".format(TBL_EDGE)
cur.executemany(Q_InsertEdgeCounts, edge_count_list)
con.commit()

#join back to link table to get geometry for display purposes and linklts for filtering purposes
Q_GeomJoin = """
    CREATE TABLE "{0}" AS
        SELECT edges.*, "{1}".cost, "{1}".geom 
        FROM (
        SELECT * FROM "{2}"
        ) AS edges 
        INNER JOIN "{1}"
        ON "{1}".mixid = edges.edge;
    COMMIT;
""".format(TBL_USE, TBL_MASTERLINKS_GEO, TBL_EDGE)
cur.execute(Q_GeomJoin)

#how many OD pairs are connected using this network?
#can help detemine value to network overall
Q_ConnectedPairs = """
    SELECT COUNT(*) FROM (SELECT DISTINCT sequence FROM "{0}") AS temp;
""".format(TBL_SPATHS)
cur.execute(Q_ConnectedPairs)
ConnectedPairs = cur.fetchall()
ConnectedPairs
#plus the sum of the count of duplicates 
#-1 to account for the one that is already counted from the shortest paths table
connected = 0
for row in pair_count:
    connected += (row[4] - 1)
#sum to find total
TotalConnected = int(ConnectedPairs[0][0]) + connected
TotalConnected
#ConnectedPairs = len(AllPaths)
#print 'Connected Pairs =' ConnectedPairs

#select the LTS 3 road segments with that would be most commonly used when included in tolerable links
#can run in qgis db manager
Q_TopLinks = """
    CREATE TABLE "{0}" AS
        SELECT edges.*, "{1}".linklts, "{1}".geom  
        FROM (
        SELECT * FROM "{2}"
        ) AS edges 
        INNER JOIN "{1}"
        ON "{1}".gid = edges.edge
        WHERE linklts > 0.3 AND linklts <= 0.6
        ORDER BY count DESC
        LIMIT 20;
    COMMIT;
""".format(TBL_TOP, TBL_ALL_LINKS, TBL_EDGE)
cur.execute(Q_TopLinks)


#################################################OLD#####################################
#quick way to view islands in QGIS
SELECT
    tl.mixid,
    tl.fromgeoff,
    tl.togeoff,
    tl.cost,
    tl.geom,
    tbl0.cnt AS cnt
FROM (
    SELECT
        edge,
        COUNT(*) AS cnt
    FROM "eg_shortestpaths"
    GROUP BY edge
) AS tbl0
INNER JOIN "eg_geo_geoffs" AS tl
ON tl.gid = tbl0.edge

