# Original
# https://github.com/dvrpc/BikeStress/blob/525dc5a5edafaac8ac90ede05262210667758a1c/networkx_multiprocessing_CHILD.py

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

num_cores = 24 # mp.cpu_count()

TBL_SPATHS = "montco_L3_shortestpaths_180"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_OD = "montco_L3_OandD"
TBL_NODENOS = "montco_L3_nodenos"
TBL_NODES_GEOFF = "montco_L3_nodes_geoff"
TBL_NODES_GID = "montco_L3_nodes_gid"
TBL_GEOFF_NODES = "montco_L3_geoff_nodes"

GROUP_NO = 180
if len(sys.argv) > 1:
    VIEW = "links_l3_grp_%s" % str(sys.argv[1])
VIEW = "links_l3_grp_%d" % GROUP_NO

IDX_nx_SPATHS_value = "montco_spaths_nx_value_idx"

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
SQL_GeoffGroup = """
WITH geoff_group AS (
    SELECT
        fromgeoff AS geoff,
        strong
    FROM "{0}"
    WHERE strong IS NOT NULL
    GROUP BY fromgeoff, strong

    UNION ALL
    SELECT
        togeoff AS geoff,
        strong
    FROM "{0}"
    WHERE strong IS NOT NULL
    GROUP BY togeoff, strong
)
SELECT geoff, strong FROM geoff_group
GROUP BY geoff, strong
ORDER BY geoff DESC;
"""
SQL_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    id integer,
    seq integer,
    ogid integer,
    dgid integer,
    edge bigint,
    rowno bigserial primary key
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
    count = 0
    start_time = time.time()
    for pair in iter(inqueue.get, sentinel):
        source, target = pair
        length, paths = nx.bidirectional_dijkstra(G, source = source, target = target, weight = 'weight')
        result.append(paths)
        count += 1
        if (count % 100) == 0:
            logger.info('{t}: {s}'.format(t = time.ctime(), s = time.time() - start_time))
            start_time = time.time()
    output.put(result)

def test_workers(pairs):
    logger.info('test_workers() started')
    result = []
    inqueue = mp.Queue()
    for source, target in pairs:
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
        result.extend(output.get())
    for proc in procs:
        proc.join()

    logger.info('test_workers() finished')
    return result

#grab master links to make graph with networkx
Q_SelectMasterLinks = SQL_SelectMasterLinks.format(VIEW)

con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

#create graph
cur.execute(Q_SelectMasterLinks)
G = nx.MultiDiGraph()
node_pairs = {}
for id, fg, tg, cost in cur.fetchall():
    G.add_edge(fg, tg, id = id, weight = cost)
    node_pairs[(fg, tg)] = id

# PID 4156
pairs = []
sentinel = None
output = mp.Queue()

# public static main(String[] args)
if __name__ == '__main__':
    logger.info('start_time: %s' % time.ctime())

#grab necessary lists and turn them into dictionaries
cur.execute(SQL_SelectAll.format(TBL_NODENOS))
nodenos = cur.fetchall()

cur.execute(SQL_SelectAll.format(TBL_NODES_GEOFF))
nodes_geoff = dict(cur.fetchall())

cur.execute(SQL_SelectAll.format(TBL_NODES_GID))
nodes_gids = dict(cur.fetchall())

cur.execute(SQL_SelectAll.format(TBL_GEOFF_NODES))
geoff_nodes = dict(cur.fetchall())

#call OD list from postgres
cur.execute(SQL_SelectAll.format(TBL_OD))
OandD = cur.fetchall()

cur.execute(SQL_GeoffGroup.format(TBL_MASTERLINKS_GROUPS))
geoff_grp = dict(cur.fetchall())

CloseEnough = []
DiffGroup = 0
NullGroup = 0

#are the OD geoffs in the same group? if so, add pair to list to be calculated
for i, (fromnodeindex, tonodeindex) in enumerate(OandD):
    #if i % pool_size == (worker_number - 1):
    fromnodeno = nodenos[fromnodeindex][0]
    tonodeno = nodenos[tonodeindex][0]
    if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
        if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
            if geoff_grp[nodes_geoff[fromnodeno]] == GROUP_NO:
                CloseEnough.append([
                    nodes_gids[fromnodeno],    # FromGID
                    #fromnodeno,                # FromNode
                    nodes_geoff[fromnodeno],  # FromGeoff
                    nodes_gids[tonodeno],      # ToGID
                    #tonodeno,                  # ToNode
                    nodes_geoff[tonodeno],    # ToGeoff
                    geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                    ])
            else:
                DiffGroup += 1
        else:
            DiffGroup += 1
    else:
        NullGroup += 1

    # del nodenos, OandD, geoff_grp, nodes_geoff

pairs = []
for i, (fgid, fgeoff, tgid, tgeoff, grp) in enumerate(CloseEnough):
    if fgeoff <> tgeoff:
        pairs.append((fgeoff, tgeoff))


# compare pairs





    paths = test_workers(pairs)

    with open(r"D:\Modeling\BikeStress\scripts\group180.cpickle", "wb") as io:
        cPickle.dump(paths, io)

    # del pairs

    with open(r"C:\Users\model-ws.DVRPC_PRIMARY\Google Drive\done.txt", "wb") as io:
        cPickle.dump("180 calculated", io)

    con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    cur.execute(Q_SelectMasterLinks)
    MasterLinks = cur.fetchall()

    node_pairs = {}
    for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
        node_pairs[(fromgeoff, togeoff)] = mixid

    # del MasterLinks

    edges = []
    for id, path in enumerate(paths):
        oGID = nodes_gids[geoff_nodes[path[0]]]
        dGID = nodes_gids[geoff_nodes[path[-1]]]
        for seq, (o ,d) in enumerate(zip(path, path[1:])):
            row = id, seq, oGID, dGID, node_pairs[(o,d)]
            edges.append(row)
    logger.info('number of records: %d' % len(edges))

    # del paths, nodes_gids, geoff_nodes, node_pairs

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

    # del edges