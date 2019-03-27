# C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\test.py

import networkx as nx
import multiprocessing as mp
import psycopg2 as psql
import json
import scipy.spatial
import time
import logging
import sys
import cPickle
logger = mp.log_to_stderr(logging.INFO)

con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_SPATHS = "shortestpaths_1_5"
TBL_MASTERLINKS_GROUPS ="master_links_grp"
TBL_NODENOS = "nodenos"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_NODES_GID = "nodes_gid"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
TBL_GEOFF_GROUP = "geoff_group"
TBL_GID_NODES = "gid_nodes"
TBL_NODE_GID = "node_gid_post"
TBL_EDGE = "edgecounts"
IDX_nx_SPATHS_value = "spaths_nx_value_idx"

TBL_TRAILS = "trail_ints"
TBL_TRAIL_NODE = "trail_node"
TBL_NODE_TRAIL = "node_trail"

TBL_TEMP_PAIRS = "temp_pairs_332_1_5"
TBL_TEMP_NETWORK = "temp_network_332_1_5"

Q_GetPairs = """
    SELECT * FROM "{0}";
    """.format(TBL_TEMP_PAIRS)
cur.execute(Q_GetPairs)
pairs = cur.fetchall()

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM public."{0}";
    """.format(TBL_TEMP_NETWORK)
    
cur.execute(Q_SelectMasterLinks)
G = nx.MultiDiGraph()
node_pairs = {}
for id, fg, tg, cost in cur.fetchall():
    G.add_edge(fg, tg, id = id, weight = cost)
    node_pairs[(fg, tg)] = id

sentinel = None
output = mp.Queue()
num_cores = 64

def worker(inqueue, output):
    result = []
    nopath = []
    count = 0
    start_time = time.time()
    for pair in iter(inqueue.get, sentinel):
        source, target = pair
        try:
            length, paths = nx.bidirectional_dijkstra(G, source = source, target = target, weight = 'weight')
        except nx.NetworkXNoPath:
            logger.info('{t}: {m}'.format(t = time.ctime(), m = "No path for {0}, {1}".format(source, target)))
            nopath.append(pair)
        except nx.NetworkXError as nxe:
            logger.info('{t}: {m}'.format(t = time.ctime(), m = "NetworkX Error: %s" % str(nxe)))
        except Exception as e:
            logger.info('{t}: {m}'.format(t = time.ctime(), m = "GENERAL ERROR: %s" % str(e)))
        else:
            result.append(paths)
        count += 1
        if (count % 100) == 0:
            logger.info('{t}: {s}'.format(t = time.ctime(), s = time.time() - start_time))
            start_time = time.time()
    output.put({'result': result, 'nopath': nopath})
    
    
def test_workers(pairs):
    logger.info('test_workers() started')
    result = []
    nopath = []
    inqueue = mp.Queue()
    for id, source, target, geom in pairs:
        inqueue.put((source, target))
    # Build O-D pair list
    # for source, target in IT.product(sources, targets):
        # inqueue.put((source, target))

    procs = []
    for i in xrange(num_cores):
        procs.append(mp.Process(target = worker, args = (inqueue, output)))
    # procs = [mp.Process(target = worker, args = (inqueue, output)) for i in range(mp.cpu_count())]

    for proc in procs:
        proc.daemon = True
        proc.start()
    for proc in procs:    
        inqueue.put(sentinel)
    for proc in procs:
        retval = output.get()
        result.extend(retval['result'])
        nopath.extend(retval['nopath'])
    for proc in procs:
        proc.join()

    logger.info('test_workers() finished')
    return result, nopath
    
if __name__ == '__main__':   
    paths, nopaths = test_workers(pairs)

    print "Length of paths:", len(paths)
    for id, path in enumerate(paths[0:5]):
        print id
        print "OID", path[0]
        print "DID", path[-1]
    # print paths[0][0]
    # print paths[0][-1]
    # print paths[0]