import networkx as nx
import multiprocessing as mp
import json
import scipy.spatial
import time
import logging
import sys
import cPickle

from database import connection

logger = mp.log_to_stderr(logging.INFO)


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
    output = mp.Queue()
    for id, source, target, geom in pairs:
        inqueue.put((source, target))
    # Build O-D pair list
    # for source, target in IT.product(sources, targets):
        # inqueue.put((source, target))
    
    num_cores = 64 # mp.cpu_count()
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

def run_child_moving_frame(i, j, log=False):
    """
        This function is imported and executed by:
            '7_CalculateShortestPaths_PARENT_MovingFrame.py'
    """

    #need in this script
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
    TBL_ALL_LINKS = "links"
    TBL_CENTS = "block_centroids"

    ####CHANGE FOR EACH TRANSIT MODE####
    TBL_BLOCK_NODE_GEOFF = "block_node_geoff"

    TBL_TEMP_PAIRS = "temp_pairs_1438_%s_%s" % (str(i), str(j))
    TBL_TEMP_NETWORK = "temp_network_1438_%s_%s" % (str(i), str(j))

    num_cores = 64 # mp.cpu_count()

    #grab master links to make graph with networkx

    Q_SelectMasterLinks = """
        SELECT
            mixid,
            fromgeoff,
            togeoff,
            cost
        FROM public."{0}";
        """.format(TBL_TEMP_NETWORK)
        
    cur = connection.cursor()

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

    if log:
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

    ipd_lookup = dict(cur.fetchall())
    
    
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
    
    Q_GetPairs = """
        SELECT * FROM "{0}";
        """.format(TBL_TEMP_PAIRS)
    cur.execute(Q_GetPairs)
    pairs = cur.fetchall()

    paths, nopaths = test_workers(pairs)
        
    with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\group1438_MF_%s_%s.cpickle" % (i, j), "wb") as io:
        cPickle.dump(paths, io)
    with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\group1438_MF_%s_%s_nopaths.cpickle" % (i, j), "wb") as io:
        cPickle.dump(nopaths, io)
    
    del pairs, nopaths
    
    cur = connection.cursor()

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
    if log:
        logger.info('number of records: %d' % len(edges))
    
    cur = connection.cursor()
    
    if (len(edges) > 0):
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
                
        with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_count_dict.pickle", "wb") as io:
            cPickle.dump(edge_count_dict, io)
            
        with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_ipd_weight.pickle", "wb") as io:
            cPickle.dump(edge_ipd_weight, io)
                
        cur = connection.cursor()

        edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
        edge_ipd_list = [(k, v) for k, v in edge_ipd_weight.iteritems()]

        if log:
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

        if log:
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
        
    if log:
        logger.info('end_time: %s' % time.ctime())
