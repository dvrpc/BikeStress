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
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_GROUPS = "montco_groups"
TBL_EDGE = "montco_L3_edgecounts_196_MF"
TBL_USE = "montco_L3_linkuse_196_MF"
TBL_TOP = "montco_L3_topLinks"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

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
""".format(TBL_USE, TBL_MASTERLINKS_GROUPS, TBL_EDGE)
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
        LIMIT 6000;
    COMMIT;
""".format(TBL_TOP, TBL_ALL_LINKS, TBL_EDGE)
cur.execute(Q_TopLinks)