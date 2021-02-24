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

from database import connection

string = "transit"

#need in this script
TBL_MASTERLINKS_GROUPS ="master_links_grp"
TBL_CENTS = "block_centroids"
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

TBL_DEST = "bus_region"
TBL_DEST_NODE = "%s_node" %string
TBL_NODE_DEST = "node_%s" %string

def worker(inqueue, output):
    result = []
    count = 0
    #start_time = time.time()
    for pair in iter(inqueue.get, sentinel):
        source, target = pair
        length, paths = nx.bidirectional_dijkstra(G, source = source, target = target, weight = 'weight')
        result.append(paths)
        count += 1
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

    #logger.info('test_workers() finished')
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
    FROM {0} s;
    """.format(selectisland)
    
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

if __name__ == '__main__':
    #logger.info('start_time: %s' % time.ctime())
    
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
    """.format(TBL_NODE_DEST)
    cur.execute(Q_GetList)
    node_dest_list = cur.fetchall()
    node_dest = dict(node_dest_list)
    
    Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_DEST_NODE)
    cur.execute(Q_GetList)
    dest_node_list = cur.fetchall()
    dest_dict = dict(dest_node_list)
	
	#grab list of block centroids ipdscores to create a lookup to be referenced later when weighting for equity
    SQL_GetBlockIPD = """SELECT gid, ipdscore FROM "{0}";""".format(TBL_CENTS)
    cur.execute(SQL_GetBlockIPD)

    ipd_lookup = dict(cur.fetchall())
    
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
        
    #with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\school_paths.cpickle", "wb") as io:
    #    cPickle.dump(paths, io)
    
    del pairs
    
    cur = connection.cursor()

    cur.execute(Q_SelectMasterLinks)
    MasterLinks = cur.fetchall()
    
    node_pairs = {}       
    for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
        node_pairs[(fromgeoff, togeoff)] = mixid
        
    del MasterLinks

    edges = []
    for id, path in enumerate(paths):
        oDID = node_dest[geoff_nodes[path[0]]]
        dGID = nodes_gids[geoff_nodes[path[-1]]]
        for seq, (o ,d) in enumerate(zip(path, path[1:])):
            row = id, seq, oDID, dGID, node_pairs[(o,d)]
            edges.append(row)
    logger.info('number of records: %d' % len(edges))
    
    cur = connection.cursor()

    if (len(edges) > 0):
        # Q_CreateOutputTable = """
            # CREATE TABLE IF NOT EXISTS public."{0}"
            # (
              # id integer,
              # seq integer,
              # otid integer,
              # dgid integer,
              # edge bigint,
              # rowno BIGSERIAL PRIMARY KEY
            # )
            # WITH (
                # OIDS = FALSE
            # )
            # TABLESPACE pg_default;

            
            # CREATE INDEX IF NOT EXISTS "{1}"
                # ON public."{0}" USING btree
                # (id, seq, otid, dgid, edge)
                # TABLESPACE pg_default;
            # COMMIT;                
        # """.format(TBL_SPATHS, IDX_nx_SPATHS_value)
        # cur.execute(Q_CreateOutputTable)

        # logger.info('inserting paths')
        # str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edges[0]))))
        # cur.execute("""BEGIN TRANSACTION;""")
        # batch_size = 10000
        # for i in xrange(0, len(edges), batch_size):
            # j = i + batch_size
            # arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edges[i:j])
            ##print arg_str
            # Q_Insert = """INSERT INTO public."{0}" (id, seq, otid, dgid, edge) VALUES {1}""".format(TBL_SPATHS, arg_str)
            # cur.execute(Q_Insert)
        # cur.execute("COMMIT;")
        
        
        dict_all_paths = {}    
        #convert edges to dictionary
        for id, seq, odid, dgid, edge in edges:
            #only count links, not turns
            if edge > 0:
                key = (odid, dgid)
                if not key in dict_all_paths:
                    dict_all_paths[key] = []
                dict_all_paths[key].append(edge)

        #how many times each OD geoff pair should be counted if used at all
        #what is ipd weight of each path based on score of just origin census blocks for transit analysis
        weight_by_od = {}
        ipd_od = {}
        for oDID, dGID in dict_all_paths.iterkeys():
            onode = dest_dict[oDID]
            dnode = gid_node[dGID]
            weight_by_od[(oDID, dGID)] = len(node_gid[dnode])
            ipd_od[(oDID, dGID)] = ipd_lookup[oDID]

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
                
        #with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_count_dict_school.pickle", "wb") as io:
        #    cPickle.dump(edge_count_dict, io)
        
        #with open(r"D:\BikePedTransit\BikeStress\phase3\phase3_pickles\edge_ipd_weight_school.pickle", "wb") as io:
        #    cPickle.dump(edge_ipd_weight, io)
                
        cur = connection.cursor()
        
        edge_count_list = [(k, v) for k, v in edge_count_dict.iteritems()]
        edge_ipd_list = [(k, v) for k, v in edge_ipd_weight.iteritems()]
        
        #logger.info('inserting counts')
        
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
        
        #logger.info('inserting ipd weights')

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
    
    del edges
        
    #logger.info('end_time: %s' % time.ctime())
