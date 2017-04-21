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



####table names to modify in subsequent runs###
TBL_ALL_LINKS = "mercer_improved_lts"
TBL_CENTS = "mercer_centroids"
TBL_LINKS = "mercer_tolerablelinks_improved_edit"
TBL_NODES = "mercer_nodes_improved_improved_edit"
TBL_SPATHS = "mercer_shortestpaths_improved_edit"
TBL_EDGE = "mercer_edgecounts_improved"
TBL_USE = "mercer_linkuse_improved"
TBL_TOP = "mercer_topLinks_improved"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#quick way to view islands in QGIS
SELECT
    tl.gid,
    tl.no,
    tl.fromnodeno,
    tl.tonodeno,
    tl.linklts,
    tl.geom,
    tbl0.cnt AS cnt
FROM (
    SELECT
        edge,
        COUNT(*) AS cnt
    FROM "eg_shortestpaths"
    GROUP BY edge
) AS tbl0
INNER JOIN "eg_geoffs" AS tl
ON tl.gid = tbl0.edge


#need to recreate OandD list
#find closest node to origin and destination points (block centroids)
Q_ClosestPoint = """
    SELECT
        "{0}".gid,
        (
            SELECT nodeno 
            FROM (
                SELECT
                    nodeno,
                    st_distance("{0}".geom, geom) AS distance
                FROM "{1}"
                ORDER BY distance ASC
                LIMIT 1
            ) AS tmp
        ) AS nodeno
    FROM "{0}";
""".format(TBL_CENTS, TBL_NODES)
cur.execute(Q_ClosestPoint)
block_cent_node = cur.fetchall()
len(block_cent_node)
#create list of combinations of closest nodes; does not include self to self
AllOandD = list(itertools.permutations(block_cent_node, 2))
len(AllOandD)

# Grab the feasible oGID to dGID paths from the shortest paths table
Q_AvailPairs = """SELECT oGID, dGID FROM "{0}" GROUP BY oGID, dGID;""".format(TBL_SPATHS)
cur.execute(Q_AvailPairs)
#save as python variable
avail_gid_pairs = cur.fetchall()

#create a temporary working db in memory through sqlite
sqlite_con = sqlite3.connect(":memory:")
#sqlite_con = sqlite3.connect(r"C:\Users\smoran\Desktop\AllOandD.sqlite")
sqlite_cur = sqlite_con.cursor()
#create a 4 column table in the sqlite db to hold AllOandD
sqlite_cur.execute("""CREATE TABLE permutations (oGID INT, oNode INT, dGID INT, dNode INT);""")
#create a 2 column table to hold the available paths contained in the shortest paths table
sqlite_cur.execute("""CREATE TABLE paths (oGID INT, dGID INT);""")
sqlite_con.commit()

#insert AllOandD into the permutations table
for i, ((oGID, oNode), (dGID, dNode)) in enumerate(AllOandD):
    sqlite_cur.execute("""INSERT INTO permutations VALUES (%d, %d, %d, %d);""" % (oGID, oNode, dGID, dNode))
#insert the available pairs list into the paths table
sqlite_cur.executemany("""INSERT INTO paths VALUES(?,?);""", avail_gid_pairs)
sqlite_con.commit()

#tblA is the number of times the same Onode and Dnodes pair exists in permutations
##possible because the same node can be closest to the centroid of more than one census block
#tblB is only OD node pairs exist more than once in the table
#tblC joins to permutations to get the oGID and dGID
#the result is then joined to the existing paths so we know how many times each path needs to be counted
#5 columns: oGID, oNode, dGID, dNode, count
sqlite_cur.execute("""SELECT
                        tblC.*
                    FROM (
                        SELECT
                            permutations.*, 
                            tblB.cnt AS cnt
                        FROM (
                            SELECT * 
                            FROM ( 
                                SELECT 
                                    oNode, 
                                    dNode, 
                                    COUNT(*) AS cnt 
                                FROM permutations 
                                GROUP BY oNode, dNode
                            ) AS tblA
                            WHERE cnt > 1
                        ) AS tblB
                        INNER JOIN permutations
                        ON tblB.oNode = permutations.oNode 
                        AND tblB.dNode = permutations.dNode
                    ) AS tblC
                    INNER JOIN paths
                    ON paths.oGID = tblC.oGID
                    AND paths.dGID = tblc.dGID
                    ORDER BY oNode, dNode""")
pair_count = sqlite_cur.fetchall()
len(pair_count)

#edge count from original table
Q_EdgeCount = """SELECT edge, COUNT(*) FROM "{0}" GROUP BY edge;""".format(TBL_SPATHS)
cur.execute(Q_EdgeCount)
edge_count = cur.fetchall()
#convert to dictionary
edge_count_dict = dict(edge_count)

#select and count the edges from the shortest path table 
Q_SelectPath = """
    SELECT edge, COUNT(*) FROM "{0}" 
    WHERE oGID = %d AND dGID = %d
    GROUP BY edge;
""".format(TBL_SPATHS)
#for each item in the pair_count list, find the edges used and increase value in dictionary by the count minus 1
for i, (oGID, oNode, dGID, dNode, cnt) in enumerate(pair_count):
    cur.execute(Q_SelectPath % (oGID, dGID))
    indiv_count = cur.fetchall()
    for link in indiv_count:
        edge_count_dict[(link[0])] += (cnt-1)
        
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
        SELECT edges.*, "{1}".linklts, "{1}".geom 
        FROM (
        SELECT * FROM "{2}"
        ) AS edges 
        INNER JOIN "{1}"
        ON "{1}".gid = edges.edge;
    COMMIT;
""".format(TBL_USE, TBL_ALL_LINKS, TBL_EDGE)
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
'''
#OLD
#sqlite_cur.execute("""SELECT permutations.*, 
#                        tblB.cnt 
#                    FROM (
#                        SELECT * 
#                        FROM ( 
#                            SELECT 
#                                oNode, 
#                                dNode, 
#                                COUNT(*) AS cnt 
#                            FROM permutations 
#                            GROUP BY oNode, dNode
#                        ) AS tblA
#                        WHERE cnt > 1
#                    ) AS tblB
#                    INNER JOIN permutations
#                    ON tblB.oNode = permutations.oNode 
#                    AND tblB.dNode = permutations.dNode;""")
#pair_count = sqlite_cur.fetchall()

avail_gid_pairs = dict([(k, None) for k in avail_gid_pairs])
asdf = []
for oGID, oNode, dGID, dNode, cnt in pair_count:
    if (oGID, dGID) in avail_gid_pairs:
        asdf.append(len(filter(lambda row:row[0] == oGID and row[2] == dGID, pair_count)))


    
    
#create dictionary to hold shortest paths for all OandD
AllPaths = {}
#query to select shortest path for each OD pair
Q_SelectPath = """
    SELECT * FROM "shortest_paths_delco"
    WHERE ogid = %d AND dgid = %d;
"""
#iterate over OandD list to pull shortest paths from table
all_splits = []
splits = []

for i, ((oGID, oNode), (dGID, dNode)) in enumerate(OandD):
    start_time = time.time()
    cur.execute(Q_SelectPath % (oGID, dGID))
    results = cur.fetchall()
    AllPaths[(i)] = results
    if len(results) > 0:
        splits.append(time.time() - start_time)
    all_splits.append(time.time() - start_time)
    if len(splits) > 10:
        break

Paths = {}
for i, ((oGID, oNode), (dGID, dNode)) in enumerate(OandD):
    start_time = time.time()
    temp = filter(lambda row:row[1] == oGID and row[2] == dGID, all_results)
    Paths[(i)] = temp
    if len(temp) > 0:
        splits.append(time.time() - start_time)
    all_splits.append(time.time() - start_time)
    if len(splits) > 10:
        break

SELECT edge, COUNT(*) FROM "shortest_paths_delco" GROUP BY edge

avg = lambda iter:sum(iter)/float(len(iter))
def avg(iter):
    return sum(iter)/float(len(iter))

filter(, all_results)

#count number of times each edge value appears in the dictionary
cnt = Counter()
for key, value in AllPaths.iteritems():
    for leg in value:
        cnt[int(leg[6])] +=1
#convert dictionary to list
counts = cnt.items()
'''
