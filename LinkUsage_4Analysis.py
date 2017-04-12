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

#status checker
SELECT (SELECT COUNT(*) AS allpairs FROM mercer_odpairs) - (SELECT COUNT(*) FROM mercer_odpairs WHERE status = 1) AS calculated;


####table names to modify in subsequent runs###
#delco_ltslinks
#delco_nodes
#shortest_paths_delco
#delco_blockcentroids
#delco_edge_counts
#delco_LinkUseNetwork
#delco_topLinks


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#need to recreate OandD list
#find closest node to origin and destination points (block centroids)
Q_ClosestPoint = """
    SELECT
        "delco_blockcentroids".gid,
        (
            SELECT nodeno 
            FROM (
                SELECT
                    nodeno,
                    st_distance("delco_blockcentroids".geom, geom) AS distance
                FROM "delco_nodes"
                ORDER BY distance ASC
                LIMIT 1
            ) AS tmp
        ) AS nodeno
    FROM "delco_blockcentroids";
"""
cur.execute(Q_ClosestPoint)
block_cent_node = cur.fetchall()
#create list of combinations of closest nodes; does not include self to self
AllOandD = list(itertools.permutations(block_cent_node, 2))

# Grab the feasible oGID to dGID paths from the shortest paths table
cur.execute("""SELECT oGID, dGID FROM "shortest_paths_delco" GROUP BY oGID, dGID;""")
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


#edge count from original table
Q_EdgeCount = """SELECT edge, COUNT(*) FROM "shortest_paths_delco" GROUP BY edge;"""
cur.execute(Q_EdgeCount)
edge_count = cur.fetchall()
#convert to dictionary
edge_count_dict = dict(edge_count)

#select and count the edges from the shortest path table 
Q_SelectPath = """
    SELECT edge, COUNT(*) FROM "shortest_paths_delco" 
    WHERE oGID = %d AND dGID = %d
    GROUP BY edge;
"""
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
    CREATE TABLE IF NOT EXISTS public.delco_edge_counts
    (
        edge integer,
        count integer
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
"""
cur.execute(Q_CreateEdgeCountTable)
con.commit()

#insert value into that edge count table
Q_InsertEdgeCounts = """
    INSERT INTO delco_edge_counts VALUES (%s, %s);
"""
cur.executemany(Q_InsertEdgeCounts, edge_count_list)
con.commit()

#join back to link table to get geometry for display purposes and linklts for filtering purposes
Q_GeomJoin = """
    CREATE TABLE delco_LinkUseNetwork AS
        SELECT edges.*, "delco_ltslinks".linklts, "delco_ltslinks".geom 
        FROM (
        SELECT * FROM "delco_edge_counts"
        ) AS edges 
        INNER JOIN "delco_ltslinks"
        ON "delco_ltslinks".gid = edges.edge;
    COMMIT;
"""
cur.execute(Q_GeomJoin)

#how many OD pairs are connected using this network?
#can help detemine value to network overall
Q_ConnectedPairs = """
    SELECT COUNT(*) FROM (SELECT DISTINCT sequence FROM shortest_paths_delco) AS temp;
"""
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
    CREATE TABLE delco_topLinks AS
        SELECT edges.*, "delco_ltslinks".linklts, "delco_ltslinks".geom  
        FROM (
        SELECT * FROM "delco_edge_counts"
        ) AS edges 
        INNER JOIN "delco_ltslinks"
        ON "delco_ltslinks".gid = edges.edge
        WHERE linklts > 0.3 AND linklts <= 0.6
        ORDER BY count DESC
        LIMIT 20;
    COMMIT;
"""
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
