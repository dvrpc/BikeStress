import multiprocessing
import multiprocessing.dummy
# import threading
import Queue
import psycopg2 as psql
import time
import logging
import csv
import itertools
import numpy
import time
import sys
import pickle
import cPickle
import sqlite3
from collections import Counter
import json
import scipy.spatial
import networkx as nx
logger = multiprocessing.log_to_stderr(logging.INFO)

TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco_L3_tolerablelinks"
TBL_SPATHS = "montco_L3_shortestpaths_180"
TBL_TOLNODES = "montco_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_geoffs"
TBL_GEOFF_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_master_links_grp"
TBL_GROUPS = "montco_groups"
TBL_EDGE = "montco_L3_edgecounts_original180"
TBL_USE = "montco_L3_linkuse_original180"
TBL_TOP = "montco_L3_topLinks"

# class _Worker(threading.Thread):
    # def __init__(self, queue, offset, batch_size):
        # threading.Thread.__init__(self)
        # self.queue = queue
        # self.offset = offset
        # self.batch_size = batch_size

    # def run(self):
        # con = psql.connect(dbname = "BikeStress", host = "toad", port = 5432, user = "postgres", password = "sergt")
        # cur = con.cursor()
        # cur.execute("""SELECT ogid, dgid, edge FROM public."montco_L3_shortestpaths" LIMIT {1} OFFSET {0};""".format(self.offset, self.batch_size))
        
        # temp_path_dict = {}
        # for ogid, dgid, edge in cur.fetchall():
            # if edge > 0:
                # key = (ogid, dgid)
                # if not key in temp_path_dict:
                    # temp_path_dict[key] = []
                # temp_path_dict[key].append(edge)
        # self.queue.put(temp_path_dict)

def worker(args):
    offset, batch_size = args
    logger.info("Offset %d" % offset)

    con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()
    # cur.execute("""SELECT ogid, dgid, edge FROM public."montco_L3_shortestpaths" ORDER BY id, seq LIMIT {1} OFFSET {0};""".format(offset, batch_size))
    # cur.execute("""DISCARD TEMP;""")
    
        # BEGIN;
        # SET LOCAL work_mem = '1000MB';
    # cur.execute("""
        # SELECT
            # ogid,
            # dgid,
            # edge
        # FROM "montco_L3_shortestpaths"
        # WHERE rowno >= {0}
        # LIMIT {1} 
        # OFFSET {0};
    # """.format(offset, batch_size))

    cur.execute("""
        SELECT
            ogid,
            dgid,
            edge
        FROM "{2}"
        WHERE rowno >= {0}
        ORDER BY rowno ASC
        LIMIT {1}
    """.format(offset, batch_size, TBL_SPATHS))
    
    results = cur.fetchall()
    # con.rollback()
    con.close()
    del con, cur
    # time.sleep(5)

    temp_path_dict = {}
    for ogid, dgid, edge in results:
        if edge > 0:
            key = (ogid, dgid)
            if not key in temp_path_dict:
                temp_path_dict[key] = []
            temp_path_dict[key].append(edge)
    # queue.put(temp_path_dict)
    # con.rollback()
    # con.close()
    return temp_path_dict

if __name__ == "__main__":
    print time.ctime()
    nworkers = 64
    pool = multiprocessing.Pool(nworkers)
    batch_size = 1000000L
    i = 0L
    i = 0L
    j = 0L

    work_units = []
    while (i < 2147678205L):
    #while (i < 2147678205L):
        work_units.append((i, batch_size))
        i += batch_size
        j += 1
        # if j > 128:
            # break

    # Willnote: This is bizarre
    # Edit: OK - using OFFSET will force Postgres to manually read through the
    # entire table until the offset is reached. If the table size exceeds the
    # allocated memory, the table will be cached to disk.
    # Normally this isn't too much of a problem since table sizes are not
    # massive, unlike this 2.18 billion row monster table.
    # 
    # The implemented solution was to create a BIGSERIAL (since the number of
    # rows exceeds the 32-bit signed integer [2^(32-1)] limit. Then replace
    # OFFSET with a combination of 'WHERE rowno > [n] ORDER BY rowno ASC'.

    dict_all_paths = {}
    # for i in xrange(0, len(work_units), nworkers):
        # logger.info("%s: Offset = %s" % (time.ctime(), str(work_units[i][0])))
        # j = i + nworkers

    results = pool.map(worker, work_units)
    for _result in results:
        for k, v in _result.iteritems():
            if k in dict_all_paths:
                dict_all_paths[k].extend(v)
            else:
                dict_all_paths[k] = v
        # cur.execute("CHECKPOINT")
        # time.sleep(5)

    print len(dict_all_paths)
    print sum([len(v) for v in dict_all_paths.itervalues()])
    print time.ctime()
    
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

    node_gid = {}
    gid_node = {}
    for GID, nodeno in results:
        if not nodeno in node_gid:
            node_gid[nodeno] = []
        if GID in gid_node:
            print "Warn %d" % GID
        node_gid[nodeno].append(GID)
        gid_node[GID] = nodeno
    
    print time.ctime(), "weight_by_od"
    weight_by_od = {}
    for oGID, dGID in dict_all_paths.iterkeys():
        onode = gid_node[oGID]
        dnode = gid_node[dGID]
        weight_by_od[(oGID, dGID)] = len(node_gid[onode]) * len(node_gid[dnode])

    print time.ctime(), "edge_count_dict"
    edge_count_dict = {}
    for key, paths in dict_all_paths.iteritems():
        path_weight = weight_by_od[key]
        for edge in paths:
            if not edge in edge_count_dict:
                edge_count_dict[edge] = 0
            edge_count_dict[edge] += path_weight
            
    with open(r"D:\Modeling\BikeStress\edge_count_dict_orig180.pickle", "wb") as io:
        cPickle.dump(edge_count_dict, io)
            
    con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()
    edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
    str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edge_count_list[0]))))
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(edge_count_list), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edge_count_list[i:j])
        Q_Insert = """INSERT INTO "{0}" VALUES {1};""".format(TBL_EDGE, arg_str)
        cur.execute(Q_Insert)
    cur.execute("COMMIT;")
    con.commit()
