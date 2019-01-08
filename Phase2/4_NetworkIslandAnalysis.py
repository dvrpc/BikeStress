#run thru cmd
#C:\Users\model-ws>AppData\Local\Continuum\Anaconda2\python.exe #D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\4_NetworkIslandAnalysis.py

import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys
import json
import scipy.spatial
import networkx as nx

#table names
TBL_ALL_LINKS = "links_testarea"
TBL_CENTS = "blockcentroids_testarea"
TBL_LINKS = "tolerablelinks_testarea"
TBL_NODES = "nodes_testarea"
TBL_TOLNODES = "tol_nodes_testarea"
TBL_GEOFF_LOOKUP = "geoffs_testarea"
TBL_GEOFF_LOOKUP_GEOM = "geoffs_viageom_testarea"
TBL_MASTERLINKS = "master_links_testarea"
TBL_MASTERLINKS_GEO = "master_links_geo_testarea"
TBL_MASTERLINKS_GROUPS = "master_links_grp_testarea"
TBL_GROUPS = "groups_testarea"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "tolerableturns_testarea"

TBL_NODENOS = "nodenos_testarea"
TBL_NODES_GEOFF = "nodes_geoff_testarea"
TBL_NODES_GID = "nodes_gid_testarea"
TBL_GEOFF_NODES = "geoff_nodes_testarea"
TBL_OD = "OandD_testarea"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_testarea"
TBL_GEOFF_GROUP = "geoff_group_testarea"

IDX_GEOFF_GROUP = "geoff_group_value_idx_testarea"
IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx_testarea"
IDX_NODENOS = "nodeno_idx_testarea"
IDX_NODES_GEOFF = "nodes_geoff_idx_testarea"
IDX_NODES_GID = "nodes_gid_idx_testarea"
IDX_GEOFF_NODES = "geoff_nodes_idx_testarea"
IDX_OD_value = "od_value_idx_testarea"

con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


class Sponge:
    def __init__(self, *args, **kwds):
        self._args = args
        self._kwds = kwds
        for k, v in kwds.iteritems():
            setattr(self, k, v)
        self.code = 0
        self.groups = {}
        
def Group(groupNo, subgraphs, counter):
    for i, g in enumerate(subgraphs):
        for (fg, tg) in g.edges():
            if (fg, tg) in links:
                links[(fg, tg)].groups[groupNo] = counter
            if (tg, fg) in links:
                links[(tg, fg)].groups[groupNo] = counter
        counter += 1
    return counter

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM "{0}";
    """.format(TBL_MASTERLINKS_GEO)
cur.execute(Q_SelectMasterLinks)
#format master links so networkx can read them
h = ['id','fg','tg','cost']
links = {}
for row in cur.fetchall():
    l = Sponge(**dict(zip(h, row)))
    links[(l.fg, l.tg)] = l

print time.ctime(), "Creating Graph"
#create graph
G = nx.MultiDiGraph()
for l in links.itervalues():
    G.add_edge(l.fg, l.tg)

print time.ctime(), "Find Islands"
counter = 0
counter = Group(0, list(nx.strongly_connected_component_subgraphs(G)), counter)
counter = Group(1, list(nx.weakly_connected_component_subgraphs(G)), counter)
counter = Group(2, list(nx.attracting_component_subgraphs(G)), counter)
counter = Group(3, list(nx.connected_component_subgraphs(G.to_undirected())), counter)

results = []
for l in links.itervalues():
    row = [l.id, l.fg, l.tg]
    for i in xrange(4):
        if i in l.groups:
            row.append(l.groups[i])
        else:
            row.append(None)
    results.append(row)

#get groups back into postgis
Q_CreateLinkGrpTable = """
    CREATE TABLE public."{0}"(
        mixid integer, 
        fromgeoff integer, 
        togeoff integer, 
        strong integer,
        weak integer,
        attracting integer,
        undirected integer
    );""".format(TBL_GROUPS)
cur.execute(Q_CreateLinkGrpTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(results[0]))))
arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)

Q_InsertGrps = """
INSERT INTO
    public."{0}"
VALUES {1}
""".format(TBL_GROUPS, arg_str)

cur.execute(Q_InsertGrps)
con.commit()
del results

#join strong group number to master links geo
Q_JoinGroupGeo = """
    CREATE TABLE public."{1}" AS(
        SELECT * FROM(
            SELECT 
                t1.*,
                t0.strong
            FROM "{0}" AS t0
            LEFT JOIN "{2}" AS t1
            ON t0.mixid = t1.mixid
        ) AS "{1}"
    );""".format(TBL_GROUPS, TBL_MASTERLINKS_GROUPS, TBL_MASTERLINKS_GEO)
cur.execute(Q_JoinGroupGeo)
con.commit()

# Q_CREATEINDEX = """
# CREATE INDEX montco_master_links_grp_idx
   # ON public.montco_master_links_grp (strong ASC NULLS LAST);
# """


#query to find min and max number of strong
Q_StrongSelect = """
    SELECT strong FROM "{0}"
    WHERE strong > 1
    ;""".format(TBL_MASTERLINKS_GROUPS)
cur.execute(Q_StrongSelect)
strong_grps = cur.fetchall()

print time.ctime(), "Create Group Views"
##iterate over groups
#Q_CreateView = """CREATE VIEW %s AS(
#                    SELECT * FROM "{0}"
#                    WHERE strong = %d)""".format(TBL_MASTERLINKS_GROUPS)
#for grpNo in xrange(0, max(strong_grps)[0]):
#    tblname = "links_grp_%d" % grpNo
#    cur.execute("""DROP VIEW IF EXISTS %s;""" % tblname)
#    #create view for each group
#    cur.execute(Q_CreateView % (tblname, grpNo))

#for level 3 analysis
Q_CreateView = """CREATE VIEW %s AS(
    SELECT * FROM "{0}"
    WHERE strong = %d)
""".format(TBL_MASTERLINKS_GROUPS)
for grpNo in xrange(min(strong_grps)[0], max(strong_grps)[0]):
    tblname = "links_uc_grp_%d" % grpNo
    cur.execute("""DROP VIEW IF EXISTS %s;""" % tblname)
    #create view for each group
    cur.execute(Q_CreateView % (tblname, grpNo))
    
    
    
SQL_GetGeoffs = """SELECT geoffid, vianode, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_GEOFF_LOOKUP_GEOM)
SQL_GetBlocks = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_CENTS)

def GetCoords(record):
    id, vianode, geojson = record
    return id, vianode, json.loads(geojson)['coordinates']
def ExecFetchSQL(SQL_Stmt):
    cur = con.cursor()
    cur.execute(SQL_Stmt)
    return map(GetCoords, cur.fetchall())
    
#create OD list
data = ExecFetchSQL(SQL_GetGeoffs)
world_ids, world_vias, world_coords = zip(*data)
#remove extra brackets from coords list and replace with new list going forward
wc = []
for i in xrange(0, len(world_coords)):
    wc.append(world_coords[i][0])
    
node_coords = dict(zip(world_vias, wc))
geoff_nodes = dict(zip(world_ids, world_vias))
# Node to Geoff dictionary (a 'random' geoff will be selected for each node)
nodes_geoff = dict(zip(world_vias, world_ids))
geofftree = scipy.spatial.cKDTree(wc)
del world_coords, wc, world_vias
#print len(world_ids)
#print type(world_ids)
#print world_ids[0:10]

data = ExecFetchSQL(SQL_GetBlocks)
results = []
for i, (id, _, coord) in enumerate(data):
    dist, index = geofftree.query(coord)
    #index comes out as type = numpy.ndarray and needs to be called as such
    geoffid = world_ids[numpy.ndarray.item(index)]
    nodeno = geoff_nodes[geoffid]
    results.append((id, nodeno))
del data, world_ids
    
gids, nodenos = zip(*results)
#nodenos = sorted(set(nodenos))
# Node to GID dictionary (a 'random' GID will be selected for each node)
nodes_gids = dict(zip(nodenos, gids))
nodetree = scipy.spatial.cKDTree(map(lambda nodeno:node_coords[nodeno], nodenos))
sdm = nodetree.sparse_distance_matrix(nodetree, 8046.72)
del gids, nodetree, results

OandD = sorted(sdm.keys())
del sdm

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
geoff_grp_list = cur.fetchall()
geoff_grp = dict(geoff_grp_list)

CloseEnough = []
DiffGroup = 0
NullGroup = 0
#are the OD geoffs in the same group? if so, add pair to list to be calculated
for i, (fromnodeindex, tonodeindex) in enumerate(OandD):
    #if i % pool_size == (worker_number - 1):
    fromnodeno = nodenos[fromnodeindex]
    tonodeno = nodenos[tonodeindex]
    if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
        if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
            # if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
                CloseEnough.append([
                    nodes_gids[fromnodeno],    # FromGID
                    fromnodeno,                # FromNode
                    nodes_geoff[fromnodeno],   # FromGeoff
                    nodes_gids[tonodeno],      # ToGID
                    tonodeno,                  # ToNode
                    nodes_geoff[tonodeno],     # ToGeoff
                    geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                    ])
        else:
            DiffGroup += 1
    else:
        NullGroup += 1

# write OandD into a table in postgres to refer to later
# Q_CreateOutputTable = """
# CREATE TABLE IF NOT EXISTS public."{0}"
# (
    # origin integer,
    # destination integer
# )
# WITH (
    # OIDS = FALSE
# )
# TABLESPACE pg_default;
# COMMIT;

# CREATE INDEX IF NOT EXISTS "{1}"
    # ON public."{0}" USING btree
    # (origin, destination)
    # TABLESPACE pg_default;    
# """.format(TBL_OD, IDX_OD_value)
# cur.execute(Q_CreateOutputTable)

# str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(OandD[0]))))
# cur.execute("""BEGIN TRANSACTION;""")
# batch_size = 10000
# for i in xrange(0, len(OandD), batch_size):
    # j = i + batch_size
    # arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in OandD[i:j])
    # #print arg_str
    # Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_OD, arg_str)
    # cur.execute(Q_Insert)
# cur.execute("COMMIT;")

#convert nodes_geoff, and nodes_gids dictionaries to list and save to tables in postgres
nodes_geoff_list = [(k, v) for k, v in nodes_geoff.iteritems()]
nodes_gids_list = [(k, v) for k, v in nodes_gids.iteritems()]
geoff_nodes_list = [(k, v) for k, v in geoff_nodes.iteritems()]


#write these plus nodesnos list into postgres to call later to turn back into a dictionary


#write nodenos into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    nodes integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (nodes)
    TABLESPACE pg_default;    
""".format(TBL_NODENOS, IDX_NODENOS)
cur.execute(Q_CreateOutputTable)

Q_Insert = """INSERT INTO public."{0}" VALUES (%s)""".format(TBL_NODENOS)
cur.executemany(Q_Insert, map(lambda v:(v,), nodenos))
con.commit()
            

#write node_geoffs into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    nodes integer,
    geoffid integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (nodes, geoffid)
    TABLESPACE pg_default;    
""".format(TBL_NODES_GEOFF, IDX_NODES_GEOFF)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(nodes_geoff_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(nodes_geoff_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in nodes_geoff_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_NODES_GEOFF, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

#write node_gids into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    nodes integer,
    gid integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (nodes)
    TABLESPACE pg_default;    
""".format(TBL_NODES_GID, IDX_NODES_GID)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(nodes_gids_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(nodes_gids_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in nodes_gids_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_NODES_GID, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

#write geoff_nodes into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    geoffid integer,
    nodes integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (nodes, geoffid)
    TABLESPACE pg_default;    
""".format(TBL_GEOFF_NODES, IDX_GEOFF_NODES)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(geoff_nodes_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(geoff_nodes_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in geoff_nodes_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_GEOFF_NODES, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")
