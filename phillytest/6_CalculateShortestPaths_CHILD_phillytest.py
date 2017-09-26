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

#don't actually need
TBL_ALL_LINKS = "uc_testlinks"
TBL_CENTS = "uc_testcentroids"
TBL_LINKS = "ucity_tolerablelinks"
TBL_NODES = "sa_nodes"
TBL_TOLNODES = "ucity_tol_nodes"
TBL_GEOFF_LOOKUP = "geoffs_uc"
TBL_GEOFF_GEOM = "geoffs_viageom_uc"
TBL_MASTERLINKS = "master_links_geo_uc"
TBL_MASTERLINKS_GROUPS = "master_links_grp_uc"
TBL_GROUPS = "groups_uc"

#need in this script
TBL_SPATHS = "shortestpaths_uc"
TBL_MASTERLINKS_GROUPS = "master_links_grp_uc"
# TBL_OD = "OandD"
TBL_NODENOS = "nodenos_uc"
TBL_NODES_GEOFF = "nodes_geoff_uc"
TBL_NODES_GID = "nodes_gid_uc"
TBL_GEOFF_NODES = "geoff_nodes_uc"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_uc"
TBL_GEOFF_GROUP = "geoff_group_uc"
IDX_nx_SPATHS_value = "spaths_nx_value_idx_uc"


# island 196 test
# TBL_SPATHS = "montco_L3_shortestpaths_196"
# TBL_NODENOS = "montco_L3_nodenos_2"
# TBL_NODES_GEOFF = "montco_L3_nodes_geoff_2"
# TBL_NODES_GID = "montco_L3_nodes_gid_2"
# TBL_GEOFF_NODES = "montco_L3_geoff_nodes_2"
# TBL_OD = "montco_L3_OandD_2"



VIEW = "links_uc_grp_%s" % str(sys.argv[1])



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

'''
def test_single_worker():
    result = []
    count = 0
    for source, target in IT.product(sources, targets):
        for path in nx.all_simple_paths(G, source = source, target = target,
                                        cutoff = None):
            result.append(path)
            count += 1
            if count % 10 == 0:
                logger.info('{c}'.format(c = count))
print 
    return result
'''

num_cores = 64 # mp.cpu_count()

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM public."{0}";
    """.format(VIEW)
    
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

if __name__ == '__main__':
    logger.info('start_time: %s' % time.ctime())
    
    #grab necessary lists and turn them into dictionaries
    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_NODES_GID)
    cur.execute(Q_GetList)
    nodes_gids_list = cur.fetchall()
    nodes_gids = dict(nodes_gids_list)
    
    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_GEOFF_NODES)
    cur.execute(Q_GetList)
    geoff_nodes_list = cur.fetchall()
    geoff_nodes = dict(geoff_nodes_list)
    
    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_NODENOS)
    cur.execute(Q_GetList)
    nodenos = cur.fetchall()

    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_NODES_GEOFF)
    cur.execute(Q_GetList)
    nodes_geoff_list = cur.fetchall()
    nodes_geoff = dict(nodes_geoff_list)

    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_NODES_GID)
    cur.execute(Q_GetList)
    nodes_gids_list = cur.fetchall()
    nodes_gids = dict(nodes_gids_list)

    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_GEOFF_NODES)
    cur.execute(Q_GetList)
    geoff_nodes_list = cur.fetchall()
    geoff_nodes = dict(geoff_nodes_list)

    Q_GetGroupPairs = """
        SELECT
            fromgeoff AS fgeoff,
            togeoff AS tgeoff,
            groupnumber AS grp
        FROM "{0}"
        WHERE groupnumber = {1};
        """.format(TBL_BLOCK_NODE_GEOFF, str(sys.argv[1]))
    cur.execute(Q_GetGroupPairs)
    group_pairs = cur.fetchall()
        
    pairs = []
    for i, (fgeoff, tgeoff, grp) in enumerate(group_pairs):
        source = fgeoff
        target = tgeoff
        pairs.append((source, target))
        
    paths = test_workers(pairs)
        
    with open(r"D:\Modeling\BikeStress\scripts\paths_ucity.cpickle", "wb") as io:
        cPickle.dump(paths, io)
    
    del pairs
    
    # with open(r"C:\Users\model-ws.DVRPC_PRIMARY\Google Drive\done.txt", "wb") as io:
        # cPickle.dump("180 calculated", io)
    
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
        logger.info('creating table')
        Q_CreateOutputTable = """
            CREATE TABLE IF NOT EXISTS public."{0}"
            (
              id integer,
              seq integer,
              ogid integer,
              dgid integer,
              edge bigint,
              rowno BIGSERIAL PRIMARY KEY
            )
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;

            
            CREATE INDEX IF NOT EXISTS "{1}"
                ON public."{0}" USING btree
                (id, seq, ogid, dgid, edge)
                TABLESPACE pg_default;
            COMMIT;                
        """.format(TBL_SPATHS, IDX_nx_SPATHS_value)
        cur.execute(Q_CreateOutputTable)

        logger.info('inserting records')
        str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edges[0]))))
        cur.execute("""BEGIN TRANSACTION;""")
        batch_size = 10000
        for i in xrange(0, len(edges), batch_size):
            j = i + batch_size
            arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edges[i:j])
            #print arg_str
            Q_Insert = """INSERT INTO public."{0}" (id, seq, ogid, dgid, edge) VALUES {1}""".format(TBL_SPATHS, arg_str)
            cur.execute(Q_Insert)
        cur.execute("COMMIT;")
        logger.info('end_time: %s' % time.ctime())
        
    del edges
        
    # with open(r"C:\Users\model-ws.DVRPC_PRIMARY\Google Drive\done2.txt", "wb") as io:
        # cPickle.dump("180 written to DB", io)

