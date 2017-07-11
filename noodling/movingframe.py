# Moving Frame
# https://github.com/dvrpc/BikeStress/blob/ccaf0949861b5e9affed5f125e9e0c3f5fdd2286/6_CalculateShortestPaths_CHILD.py

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

#need in this script
TBL_SPATHS = "montco_L3_shortestpaths_180_MF2"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_OD = "montco_L3_OandD"
TBL_NODENOS = "montco_L3_nodenos"
TBL_NODES_GEOFF = "montco_L3_nodes_geoff"
TBL_NODES_GID = "montco_L3_nodes_gid"
TBL_GEOFF_NODES = "montco_L3_geoff_nodes"

TBL_TEMP_PAIRS = "temp2_pairs_180_%s" % str(sys.argv[1])
TBL_TEMP_NETWORK = "temp2_network_180_%s" % str(sys.argv[1])

IDX_nx_SPATHS_value = "montco_spaths_mf2_value_idx"

SQL_SelectAll = """
SELECT * FROM "{0}";
"""
SQL_SelectMasterLinks = """
SELECT
    mixid,
    fromgeoff,
    togeoff,
    cost
FROM public."{0}";
"""
SQL_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    id integer,
    seq integer,
    ogid integer,
    dgid integer,
    edge bigint,
    rowno BIGSERIAL PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (id, seq, ogid, dgid, edge)
    TABLESPACE pg_default;
COMMIT;
"""
SQL_Insert = """INSERT INTO public."{0}" (id, seq, ogid, dgid, edge) VALUES {1}"""

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
            # logger.info('{t}: {m}'.format(t = time.ctime(), m = "No path for {0}, {1}".format(source, target)))
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

    procs = []
    for i in xrange(num_cores):
        procs.append(mp.Process(target = worker, args = (inqueue, output)))

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

num_cores = 64 # mp.cpu_count()

#grab master links to make graph with networkx

Q_SelectMasterLinks = SQL_SelectMasterLinks.format(TBL_TEMP_NETWORK)

con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

#create graph
cur.execute(Q_SelectMasterLinks)
G = nx.MultiDiGraph()
node_pairs = {}
for id, fg, tg, cost in cur.fetchall():
    G.add_edge(fg, tg, id = id, weight = cost)
    node_pairs[(fg, tg)] = id


pairs = []
sentinel = None
output = mp.Queue()

if __name__ == '__main__':
    logger.info('start_time: %s' % time.ctime())

    #grab necessary lists and turn them into dictionaries
    cur.execute(SQL_SelectAll.format(TBL_NODES_GID))
    nodes_gids = dict(cur.fetchall())

    cur.execute(SQL_SelectAll.format(TBL_GEOFF_NODES))
    geoff_nodes = dict(cur.fetchall())

    cur.execute(SQL_SelectAll.format(TBL_TEMP_PAIRS))
    pairs = cur.fetchall()

    paths, nopaths = test_workers(pairs)

    with open(r"D:\Modeling\BikeStress\scripts\group180_MF2_%s.cpickle" % sys.argv[1], "wb") as io:
        cPickle.dump(paths, io)
    with open(r"D:\Modeling\BikeStress\scripts\group180_MF2_%s_nopaths.cpickle" % sys.argv[1], "wb") as io:
        cPickle.dump(nopaths, io)

    del pairs, nopaths

    con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    cur.execute(Q_SelectMasterLinks)
    MasterLinks = cur.fetchall()

    node_pairs = {}
    for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
        node_pairs[(fromgeoff, togeoff)] = mixid

    del MasterLinks

    edges = []
    for id, path in enumerate(paths):
        oGID = nodes_gids[geoff_nodes[path[0]]]
        dGID = nodes_gids[geoff_nodes[path[-1]]]
        for seq, (o ,d) in enumerate(zip(path, path[1:])):
            row = id, seq, oGID, dGID, node_pairs[(o,d)]
            edges.append(row)
    logger.info('number of records: %d' % len(edges))

    del paths, nodes_gids, geoff_nodes, node_pairs

    if (len(edges) > 0):
        Q_CreateOutputTable = SQL_CreateOutputTable.format(TBL_SPATHS, IDX_nx_SPATHS_value)
        cur.execute(Q_CreateOutputTable)

        logger.info('inserting records')
        str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edges[0]))))
        cur.execute("""BEGIN TRANSACTION;""")
        batch_size = 10000
        for i in xrange(0, len(edges), batch_size):
            j = i + batch_size
            arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edges[i:j])
            Q_Insert = SQL_Insert.format(TBL_SPATHS, arg_str)
            cur.execute(Q_Insert)
        cur.execute("COMMIT;")
        logger.info('end_time: %s' % time.ctime())

    del edges