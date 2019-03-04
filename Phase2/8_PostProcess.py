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
TBL_ALL_LINKS = "testarea_links"
TBL_MASTERLINKS_GROUPS = "master_links_grp_testarea"
TBL_EDGE = "edgecounts_testarea"
TBL_EDGETOTAL = "edgetotals_testarea"
TBL_USE = "linkuse_testarea"
TBL_COUNTLTS = "linkuse_lts_testarea"
TBL_LTS3 = "LTS3_linkuse_testarea"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()


#sum the counts of unique edges in the edge count table that was built upon by each run of 7_CountEdges.py on the subsets of the shortest path results
Q_UniqueEdgeSum = """
    CREATE TABLE "{0}" AS
        SELECT edge, SUM(count) AS total
        FROM "{1}"
        GROUP BY edge;
    COMMIT;
""".format(TBL_EDGETOTAL, TBL_EDGE)
cur.execute(Q_UniqueEdgeSum)

#need to restore master links groups

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
""".format(TBL_USE, TBL_MASTERLINKS_GROUPS, TBL_EDGETOTAL)
cur.execute(Q_GeomJoin)

#how many OD pairs are connected using this network?
#can help detemine value to network overall

## call OD list from postgres
## will need to repeat to get all of them for a split island
#Q_GetOD = """
#    SELECT * FROM "{0}";
#    """.format(TBL_OD)
#cur.execute(Q_GetOD)
#OandD = cur.fetchall()
#
#
#Q_ConnectedPairs = """
#    SELECT COUNT(*) FROM (SELECT DISTINCT sequence FROM "{0}") AS temp;
#""".format(TBL_SPATHS)
#cur.execute(Q_ConnectedPairs)
#ConnectedPairs = cur.fetchall()
#ConnectedPairs
## plus the sum of the count of duplicates 
## -1 to account for the one that is already counted from the shortest paths table
#connected = 0
#for row in pair_count:
#    connected += (row[4] - 1)
## sum to find total
#TotalConnected = int(ConnectedPairs[0][0]) + connected
#TotalConnected
## ConnectedPairs = len(AllPaths)
## print 'Connected Pairs =' ConnectedPairs


#join results to LTS assigned links
Q_JoinLTS = """
    CREATE TABLE "{0}" AS
        SELECT use.edge, use.total, links.linklts, links.length, links.numlanes, links.bike_fac_1, links.speedtouse, use.geom
        FROM "{1}" AS use
        INNER JOIN "{2}" as links
        ON use.edge = links.gid;
    COMMIT;
""".format(TBL_COUNTLTS, TBL_USE, TBL_ALL_LINKS)
cur.execute(Q_JoinLTS)

Q_Level3Links = """
    CREATE TABLE "{0}" AS
        SELECT *
        FROM "{1}"
        WHERE linklts > 0.3 AND linklts <= 0.6;
    COMMIT;
""".format(TBL_LTS3, TBL_COUNTLTS)
cur.execute(Q_Level3Links)



