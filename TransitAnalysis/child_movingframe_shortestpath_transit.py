import networkx as nx
import multiprocessing as mp
import json
import scipy.spatial
import time
import logging
import sys
import cPickle

from database import connection

def worker(inqueue, output, G):
    # logger = mp.get_logger()
    result = []
    nopath = []
    count = 0
    sentinel = None
    start_time = time.time()
    for pair in iter(inqueue.get, sentinel):
        source, target = pair
        try:
            length, paths = nx.bidirectional_dijkstra(G, source = source, target = target, weight = 'weight')
        except nx.NetworkXNoPath:
            # logger.info('{t}: {m}'.format(t = time.ctime(), m = "No path for {0}, {1}".format(source, target)))
            nopath.append(pair)
        except nx.NodeNotFound:
            pass
        except nx.NetworkXError as nxe:
            pass
            # logger.info('{t}: {m}'.format(t = time.ctime(), m = "NetworkX Error: %s" % str(nxe)))
        except Exception as e:
            pass
            # logger.info('{t}: {m}'.format(t = time.ctime(), m = "GENERAL ERROR: %s" % str(e)))
        else:
            # logger.info('{t}: {m}'.format(t = time.ctime(), m = "path cnt: %d" % len(paths)))
            result.append(paths)
        count += 1
        if (count % 100) == 0:
            # logger.info('{t}: {c} - {s}'.format(t = time.ctime(), c = count, s = time.time() - start_time))
            start_time = time.time()
    output.put({'result': result, 'nopath': nopath})

def test_workers(pairs, G):
    # logger = mp.get_logger()
    # logger.info('test_workers() started')
    result = []
    nopath = []
    inqueue = mp.Queue()
    output = mp.Queue()
    sentinel = None
    for id, source, target, geom in pairs:
        inqueue.put((source, target))
    # Build O-D pair list
    # for source, target in IT.product(sources, targets):
        # inqueue.put((source, target))

    num_cores = 32 # mp.cpu_count()
    procs = []
    for i in xrange(num_cores):
        procs.append(mp.Process(target = worker, args = (inqueue, output, G)))
    # logger.info('test_workers() created %d workers' % len(procs))
    # procs = [mp.Process(target = worker, args = (inqueue, output)) for i in range(mp.cpu_count())]

    # logger.info('test_workers() starting workers')
    for proc in procs:
        proc.daemon = True
        proc.start()
    for proc in procs:
        inqueue.put(sentinel)
    # logger.info('test_workers() result aggregation')
    for proc in procs:
        retval = output.get()
        result.extend(retval['result'])
        nopath.extend(retval['nopath'])
    # logger.info('test_workers() joining')
    for proc in procs:
        proc.join()

    # logger.info('test_workers() finished')
    return result, nopath

def run_child_moving_frame(i, j, log=False):
    """
        This function is imported and executed by:
            '7_CalculateShortestPaths_PARENT_MovingFrame.py'
    """

    # logger = mp.log_to_stderr(logging.INFO)

    #need in this script
    TBL_MASTERLINKS_GROUPS ="master_links_grp"
    TBL_CENTS = "block_centroids"

    #bus
    string = "transit"

    #rail
    #string = "rail"

    #trolley
    #string = "trolley"

    TBL_TRANSIT_NODE = "%s_node" %string
    TBL_NODE_TRANSIT = "node_%s" %string
    TBL_NODENOS = "nodenos_%s" %string
    TBL_NODES_GEOFF = "nodes_geoff_%s" %string
    TBL_NODES_GID = "nodes_gid_%s" %string
    TBL_GEOFF_NODES = "geoff_nodes_%s" %string
    TBL_BLOCK_NODE_GEOFF = "block_node_geoff_%s" %string
    TBL_GEOFF_GROUP = "geoff_group_%s" %string
    TBL_GID_NODES = "gid_nodes_%s" %string
    TBL_NODE_GID = "node_gid_post_%s" %string

    TBL_EDGE = "edgecounts_%s" %string
    TBL_EDGE_IPD = "edges_ipd_%s" %string


    IDX_GEOFF_GROUP = "geoff_group_value_idx_%s" %string
    IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx_%s" %string
    IDX_NODENOS = "nodeno_idx_%s" %string
    IDX_NODES_GEOFF = "nodes_geoff_idx_%s" %string
    IDX_NODES_GID = "nodes_gid_idx_%s" %string
    IDX_GEOFF_NODES = "geoff_nodes_idx_%s" %string
    IDX_GID_NODES = "gid_nodes_idx_%s" %string
    IDX_OD_value = "od_value_idx_%s" %string
    IDX_NODE_GID = "node_gid_post_idx_%s" %string
    IDX_TRANSIT_NODE = "transit_node_idx_%s" %string
    IDX_NODE_TRANSIT = "node_transit_idx_%s" %string

    TBL_TEMP_PAIRS = "temp_pairs_502_%s_%s" % (str(i), str(j))
    TBL_TEMP_NETWORK = "temp_network_502_%s_%s" % (str(i), str(j))

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

    # if log:
        # logger.info('start_time: %s' % time.ctime())

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

    paths, nopaths = test_workers(pairs, G)

    # with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\group502_MF_%s_%s.cpickle" % (i, j), "wb") as io:
        # cPickle.dump(paths, io)
    # with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\group502_MF_%s_%s_nopaths.cpickle" % (i, j), "wb") as io:
        # cPickle.dump(nopaths, io)

    del pairs, nopaths

    cur = connection.cursor()

    dict_all_paths = {}
    for path in paths:
        oGID = nodes_gids[geoff_nodes[path[0]]]
        dGID = nodes_gids[geoff_nodes[path[-1]]]
        for o, d in zip(path,path[1:]):
            edge = node_pairs[(o,d)]
            if edge > 0:
                key = (oGID, dGID)
                if not key in dict_all_paths:
                    dict_all_paths[key] = []
                dict_all_paths[key].append(edge)

    if(len(dict_all_paths) > 0):
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

        # with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_count_dict.pickle", "wb") as io:
            # cPickle.dump(edge_count_dict, io)

        # with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_ipd_weight.pickle", "wb") as io:
            # cPickle.dump(edge_ipd_weight, io)

        cur = connection.cursor()

        edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
        edge_ipd_list = [(k, v) for k, v in edge_ipd_weight.iteritems()]

        # if log:
            # logger.info('inserting counts')

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
        connection.commit()

        # if log:
            # logger.info('inserting ipd weights')

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
        connection.commit()

    del paths, nodes_gids, geoff_nodes, node_pairs

    # if log:
        # logger.info('end_time: %s' % time.ctime())