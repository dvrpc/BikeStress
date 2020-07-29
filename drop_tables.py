#to drop intermediate tables between runs
import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys


#connect to DB
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


#from 5_OD_Pairs
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
TBL_NODE_TRANSIT = "node_transit"
TBL_TRANSIT_NODE = "transit_node"
TBL_NODES_GID = "nodes_gid"
TBL_GID_NODES = "gid_nodes"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_NODENOS = "nodenos"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_NODE_GID = "node_gid_post"

TablesList = [TBL_BLOCK_NODE_GEOFF,
            TBL_NODE_TRANSIT,
            TBL_TRANSIT_NODE,
            TBL_NODES_GID,
            TBL_GID_NODES,
            TBL_GEOFF_NODES,
            TBL_NODENOS,
            TBL_NODES_GEOFF,
            TBL_NODE_GID]

for table in TablesList:
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(table))
    con.commit()

#from 6_MovingFrame

TBL_NETWORK = "temp_network_332_%d_%d"
TBL_PAIRS = "temp_pairs_332_%d_%d"
TBL_NETWORK_big = "temp_network_332_%d"
TB_PAIRS_big = "temp_pairs_332_%d"

TBL_GEOFF_PAIRS = "332_geoff_pairs"
TBL_OD_LINES = "332_OD_lines"

for i in xrange(1,10):
    for j in xrange(1, 13):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    for j in xrange(101, 113):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK_big % i))
    con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS_big % i))
    con.commit()
        
for i in xrange(101,112):
    for j in xrange(1, 13):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    for j in xrange(101, 113):
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK %(i, j)))
        con.commit()
        cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS %(i, j)))
        con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_NETWORK_big % i))
    con.commit()
    cur.execute("""DROP TABLE IF EXISTS public."{0}";""".format(TBL_PAIRS_big % i))
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