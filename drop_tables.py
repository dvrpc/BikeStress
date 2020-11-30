#to drop intermediate tables between runs
import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys


#connect to DB
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


#from 5_OD_Pairs
TBL_TRANSIT_NODE = "transit_node"
TBL_NODE_TRANSIT = "node_transit"
TBL_NODENOS = "nodenos_transit"
TBL_NODES_GEOFF = "nodes_geoff_transit"
TBL_NODES_GID = "nodes_gid_transit"
TBL_GEOFF_NODES = "geoff_nodes_transit"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_transit"
TBL_GEOFF_GROUP = "geoff_group_transit"
TBL_GID_NODES = "gid_nodes_transit"
TBL_NODE_GID = "node_gid_post_transit"

TablesList = [TBL_TRANSIT_NODE,
                TBL_NODE_TRANSIT,
                TBL_NODENOS,
                TBL_NODES_GEOFF,
                TBL_NODES_GID,
                TBL_GEOFF_NODES,
                TBL_BLOCK_NODE_GEOFF,
                TBL_GEOFF_GROUP,
                TBL_GID_NODES,
                TBL_NODE_GID]

for table in TablesList:
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(table))
    con.commit()

#from 6_MovingFrame

TBL_NETWORK = "temp_network_502_%d_%d"
TBL_PAIRS = "temp_pairs_502_%d_%d"
#TBL_NETWORK_big = "temp_network_502_%d"
#TB_PAIRS_big = "temp_pairs_502_%d"

TBL_GEOFF_PAIRS = "1438_geoff_pairs"
TBL_OD_LINES = "1438_OD_lines"

for i in xrange(1,22):
    for j in xrange(1, 22):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    for j in xrange(101, 122):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()

        
for i in xrange(101,122):
    for j in xrange(1, 22):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    for j in xrange(101, 122):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()

    

for i in xrange(1,22):
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK_big %(i)))
    con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TB_PAIRS_big %(i)))
    con.commit()
for i in xrange(101, 122):
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK_big %(i)))
    con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TB_PAIRS_big %(i)))
    con.commit()

cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_GEOFF_PAIRS))
con.commit()
cur.execute("""DROP SEQUENCE IF EXISTS public."332_geoff_pairs_id_seq";""")
con.commit()
cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_OD_LINES))
con.commit()
cur.execute("""DROP SEQUENCE public."332_OD_lines_id_seq";""")
con.commit()
        

#from 7_CalculateShortest Paths
#change name of edge counts to preserve and prevent over-writing
NEW_TBL_NAME = "edgecounts_trolley"
cur.execute("""ALTER TABLE edgecounts
RENAME TO "{0}";""".format(NEW_TBL_NAME))
con.commit()