import networkx as nx
import multiprocessing as mp
import psycopg2 as psql
import json
import scipy.spatial
import time
import logging
import sys
logger = mp.log_to_stderr(logging.INFO)

TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco__L3_tolerablelinks"
TBL_NODES = "montco_nodes"
TBL_SPATHS = "montco_L3_shortestpaths"
TBL_TOLNODES = "montco__L3_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_L3_geoffs"
TBL_GEOFF_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_L3_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_GROUPS = "montco_L3_groups"
TBL_OD = "montco_L3_OandD"
TBL_NODENOS = "montco_L3_nodenos"
TBL_NODES_GEOFF = "montco_L3_nodes_geoff"
TBL_NODES_GID = "montco_L3_nodes_gid"
TBL_GEOFF_NODES = "montco_L3_geoff_nodes"


VIEW = "links_l3_grp_%s" % str(sys.argv[1])

IDX_nx_SPATHS_value = "montco_spaths_nx_value_idx"

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

num_cores = 16 # mp.cpu_count()

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM public."{0}";
    """.format(VIEW)
    
con = psql.connect(database = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
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
    
    #call OD list from postgres
    Q_GetOD = """
        SELECT * FROM "{0}";
        """.format(TBL_OD)
    cur.execute(Q_GetOD)
    OandD = cur.fetchall()

    Q_GeoffGroup = """
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
    """.format(TBL_MASTERLINKS_GROUPS)

    cur.execute(Q_GeoffGroup)
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
                if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
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
            NullGroup += 1
            
    del nodenos, OandD

    pairs = []
    for i, (fgid, fgeoff, tgid, tgeoff, grp) in enumerate(CloseEnough):
        source = fgeoff
        target = tgeoff
        pairs.append((source, target))

    paths = test_workers(pairs)
    
    cur.execute(Q_SelectMasterLinks)
    MasterLinks = cur.fetchall()
    
    node_pairs = {}       
    for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
        node_pairs[(fromgeoff, togeoff)] = mixid

    edges = []
    for id, path in enumerate(paths):
        oGID = nodes_gids[geoff_nodes[path[0]]]
        dGID = nodes_gids[geoff_nodes[path[-1]]]
        for seq, (o ,d) in enumerate(zip(path, path[1:])):
            row = id, seq, oGID, dGID, node_pairs[(o,d)]
            edges.append(row)
    logger.info('number of records: %d' % len(edges))
    
    if (len(edges) > 0):
        Q_CreateOutputTable = """
            CREATE TABLE IF NOT EXISTS public."{0}"
            (
                id integer,
                seq integer,
                ogid integer,
                dgid integer,
                edge bigint
            )
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;
            COMMIT;
            
            CREATE INDEX IF NOT EXISTS "{1}"
                ON public."{0}" USING btree
                (id, seq, ogid, dgid, edge)
                TABLESPACE pg_default;    
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
            Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_SPATHS, arg_str)
            cur.execute(Q_Insert)
        cur.execute("COMMIT;")
        logger.info('end_time: %s' % time.ctime())


#        Q_INSERT = """
#        INSERT INTO public."{0}" VALUES (%s, %s, %s, %s, %s)
#        """.format(TBL_SPATHS)
#        cur.executemany(Q_INSERT, edges)
#        con.commit()
    
    # test_single_worker()
    # assert set(map(tuple, test_workers())) == set(map(tuple, test_single_worker()))