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
TBL_SPATHS = "shortestpaths_%s_%s" % (str(sys.argv[1]), str(sys.argv[2]))
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

TBL_TEMP_PAIRS = "temp_pairs_332_%s_%s" % (str(sys.argv[1]), str(sys.argv[2]))
TBL_TEMP_NETWORK = "temp_network_332_%s_%s" % (str(sys.argv[1]), str(sys.argv[2]))




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
    """.format(TBL_TEMP_NETWORK)
    
con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
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
    nodes_gids = {}
    for nodes, gids in nodes_gids_list:
        key = nodes
        if not key in nodes_gids:
            nodes_gids[key] = []
        nodes_gids[key].append(gids)
    
    Q_GetList = """
        SELECT * FROM "{0}";
        """.format(TBL_GEOFF_NODES)
    cur.execute(Q_GetList)
    geoff_nodes_list = cur.fetchall()
    geoff_nodes = {}
    for geoff, node in geoff_nodes_list:
        key = geoff
        if not key in geoff_nodes:
            geoff_nodes[key] = []
        geoff_nodes[key].append(node)
    
    Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_GID_NODES)
    cur.execute(Q_GetList)
    gid_node_list = cur.fetchall()
    gid_node = {}
    for gid, node in gid_node_list:
        key = gid
        if not key in gid_node:
            gid_node[key] = []
        gid_node[key].append(node)
    
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
        
    Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODE_TRAIL)
    cur.execute(Q_GetList)
    node_trail_list = cur.fetchall()
    node_trail = {}
    for node, trail in node_trail_list:
        key = node
        if not key in node_trail:
            node_trail[key] = []
        node_trail[key].append(trail)
    
    Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_TRAIL_NODE)
    cur.execute(Q_GetList)
    trail_node_list = cur.fetchall()
    trail_dict = {}
    for trail, node in trail_node_list:
        key = trail
        if not key in trail_dict:
            trail_dict[key] = []
        trail_dict[key].append(node)
    
    Q_GetPairs = """
        SELECT * FROM "{0}";
        """.format(TBL_TEMP_PAIRS)
    cur.execute(Q_GetPairs)
    pairs = cur.fetchall()

    paths, nopaths = test_workers(pairs)
        
    with open(r"D:\BikePedTransit\BikeStress\scripts\phase2_pickles\group332_MF_%s_%s.cpickle" % (sys.argv[1], sys.argv[2]), "wb") as io:
        cPickle.dump(paths, io)
    with open(r"D:\BikePedTransit\BikeStress\scripts\phase2_pickles\group332_MF_%s_%s_nopaths.cpickle" % (sys.argv[1], sys.argv[2]), "wb") as io:
        cPickle.dump(nopaths, io)
    
    del pairs, nopaths

    con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    cur.execute(Q_SelectMasterLinks)
    MasterLinks = cur.fetchall()
    
    node_pairs = {}       
    for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
        node_pairs[(fromgeoff, togeoff)] = mixid
        
    del MasterLinks

    edges = []
    for id, path in enumerate(paths):
        oTID = node_trail[geoff_nodes[path[0]][0]][0]
        dGID = nodes_gids[geoff_nodes[path[-1]][0]][0]
        for seq, (o ,d) in enumerate(zip(path, path[1:])):
            row = id, seq, oTID, dGID, node_pairs[(o,d)]
            edges.append(row)
    logger.info('number of records: %d' % len(edges))
    
    con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()
    
    if (len(edges) > 0):
        Q_CreateOutputTable = """
            CREATE TABLE IF NOT EXISTS public."{0}"
            (
              id integer,
              seq integer,
              otid integer,
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
                (id, seq, otid, dgid, edge)
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
            Q_Insert = """INSERT INTO public."{0}" (id, seq, otid, dgid, edge) VALUES {1}""".format(TBL_SPATHS, arg_str)
            cur.execute(Q_Insert)
        cur.execute("COMMIT;")
        
        
        dict_all_paths = {}    
        #convert edges to dictionary
        for id, seq, otid, dgid, edge in edges:
            #only count links, not turns
            if edge > 0:
                key = (otid, dgid)
                if not key in dict_all_paths:
                    dict_all_paths[key] = []
                dict_all_paths[key].append(edge)

        #how many times each OD geoff pair should be counted if used at all
        weight_by_od = {}
        for oTID, dGID in dict_all_paths.iterkeys():
            onode = trail_dict[oTID]
            dnode = gid_node[dGID][0]
            weight_by_od[(oTID, dGID)] = len(node_gid[dnode])

        edge_count_dict = {}
        for key, paths in dict_all_paths.iteritems():
            path_weight = weight_by_od[key]
            for edge in paths:
                if not edge in edge_count_dict:
                    edge_count_dict[edge] = 0
                edge_count_dict[edge] += path_weight
                
        with open(r"D:\BikePedTransit\BikeStress\scripts\phase2_pickles\edge_count_dict_332.pickle", "wb") as io:
            cPickle.dump(edge_count_dict, io)
                
        con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
        cur = con.cursor()
        
        edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
        
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

    del paths, nodes_gids, geoff_nodes, node_pairs
    
    del edges
        
    logger.info('end_time: %s' % time.ctime())

