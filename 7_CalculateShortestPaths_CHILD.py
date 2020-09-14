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
TBL_SPATHS = "shortestpaths_%s" % str(sys.argv[1])
TBL_MASTERLINKS_GROUPS ="master_links_grp"
TBL_NODENOS = "nodenos"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_NODES_GID = "nodes_gid"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_GEOFF_GROUP = "geoff_group"
TBL_GID_NODES = "gid_nodes"
TBL_NODE_GID = "node_gid_post"
TBL_EDGE = "edgecounts"
TBL_EDGE_IPD = "edges_ipd"
TBL_CENTS = "block_centroids"
IDX_nx_SPATHS_value = "spaths_nx_value_idx"

TBL_BLOCK_NODE_GEOFF = "block_node_geoff"


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

#select query to create what used to be Views of each island individually
selectisland = """(SELECT * FROM master_links_grp WHERE strong = %d)""" % int(sys.argv[1])

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM {0} view;
    """.format(selectisland)
    
con = psql.connect(database = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
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
    """.format(TBL_GID_NODES)
    cur.execute(Q_GetList)
    gid_node_list = cur.fetchall()
    gid_node = dict(gid_node_list)
    
    #grab list of block centroids ipdscores to create a lookup to be referenced later when weighting for equity
    SQL_GetBlockIPD = """SELECT gid, ipdscore FROM "{0}";""".format(TBL_CENTS)
    cur.execute(SQL_GetBlockIPD)

    ipd_lookup = ipd_lookup = dict(cur.fetchall())

    
    Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODE_GID)
    cur.execute(Q_GetList)
    node_gid_list = cur.fetchall()
    node_gid = {}
    for node, gid in node_gid_list:
        key = node
        if not key in node_gid:
            node_gid[key] = []
        node_gid[key].append(gid)
    
    Q_GetGroupPairs = """
        SELECT
            fromgeoff AS fgeoff,
            togeoff AS tgeoff,
            groupnumber AS grp
        FROM "{0}"
        WHERE groupnumber = {1};
        """.format(TBL_BLOCK_NODE_GEOFF, int(sys.argv[1]))
    cur.execute(Q_GetGroupPairs)
    group_pairs = cur.fetchall()
        
    pairs = []
    for i, (fgeoff, tgeoff, grp) in enumerate(group_pairs):
        source = fgeoff
        target = tgeoff
        pairs.append((source, target))
        
    paths = test_workers(pairs)
        
    with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\paths.cpickle", "wb") as io:
        cPickle.dump(paths, io)
    
    del pairs
    
    con = psql.connect(database = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
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
    
    con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    if (len(edges) > 0):
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
        
        
        dict_all_paths = {}    
        #convert edges to dictionary
        for id, seq, ogid, dgid, edge in edges:
            #only count links, not turns
            if edge > 0:
                key = (ogid, dgid)
                if not key in dict_all_paths:
                    dict_all_paths[key] = []
                dict_all_paths[key].append(edge)

        #how many times each OD geoff pair should be counted if used at all
        #what is ipd weight of each path based on score of O and D census blocks
        weight_by_od = {}
        ipd_od = {}
        for oGID, dGID in dict_all_paths.iterkeys():
            onode = gid_node[oGID]
            dnode = gid_node[dGID]
            weight_by_od[(oGID, dGID)] = len(node_gid[onode]) * len(node_gid[dnode])
            ipd_od[(oGID, dGID)] = ipd_lookup[oGID] + ipd_lookup[dGID]

        edge_count_dict = {}
        edge_ipd_weight = {}
        for key, paths in dict_all_paths.iteritems():
            path_weight = weight_by_od[key]
            ipd_weight = ipd_od[key]
            for edge in paths:
                if not edge in edge_count_dict:
                    edge_count_dict[edge] = 0
                edge_count_dict[edge] += path_weight
                if not edge in edge_ipd_weight:
                    edge_ipd_weight[edge] = 0
                try:
                    edge_ipd_weight[edge] += ipd_weight
                except TypeError:
                    print edge_ipd_weight[edge], ipd_weight
                #edge_ipd_weight[edge] += ipd_weight
                
        with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_count_dict.pickle", "wb") as io:
            cPickle.dump(edge_count_dict, io)
            
        with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_ipd_weight.pickle", "wb") as io:
            cPickle.dump(edge_ipd_weight, io)
                
        con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
        cur = con.cursor()

        edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
        edge_ipd_list = [(k, v) for k, v in edge_ipd_weight.iteritems()]

        logger.info('inserting counts')

        Q_CreateOutputTable2 = """
            CREATE TABLE IF NOT EXISTS public."{0}"
            (
              edge integer,
              count integer
            )
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;

            COMMIT;                
        """.format(TBL_EDGE)
        cur.execute(Q_CreateOutputTable2)

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

        logger.info('inserting ipd weights')

        Q_CreateOutputTable3 = """
            CREATE TABLE IF NOT EXISTS public."{0}"
            (
              edge integer,
              ipdweight integer
            )
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;

            COMMIT;                
        """.format(TBL_EDGE_IPD)
        cur.execute(Q_CreateOutputTable3)

        str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edge_ipd_list[0]))))
        cur.execute("""BEGIN TRANSACTION;""")
        batch_size = 10000
        for i in xrange(0, len(edge_ipd_list), batch_size):
            j = i + batch_size
            arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edge_ipd_list[i:j])
            Q_Insert = """INSERT INTO "{0}" VALUES {1};""".format(TBL_EDGE_IPD, arg_str)
            cur.execute(Q_Insert)
        cur.execute("COMMIT;")
        con.commit()
        
        
        

    del paths, nodes_gids, geoff_nodes, node_pairs
    
    del edges
        
    logger.info('end_time: %s' % time.ctime())
