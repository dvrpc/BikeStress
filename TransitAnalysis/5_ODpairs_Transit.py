#run thru cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\TransitAnalysis\5_ODpairs_Transit.py

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
TBL_ALL_LINKS = "links"
TBL_CENTS = "block_centroids"
TBL_ALLCENTS = "block_centroids"
TBL_LINKS = "tolerablelinks"
TBL_NODES = "nodes"
TBL_TOLNODES = "tol_nodes"
TBL_GEOFF_LOOKUP = "geoffs"
TBL_GEOFF_LOOKUP_GEOM = "geoffs_viageom"
TBL_MASTERLINKS = "master_links"
TBL_MASTERLINKS_GEO = "master_links_geo"
TBL_MASTERLINKS_GROUPS = "master_links_grp"
TBL_GROUPS = "groups"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "tolerableturns"
TBL_BRIDGECENTS = "bridge_buffer_cents"

####CHANGE FOR EACH TRANSIT MODE####
#point shapefile with gid and geom
TBL_TRANSIT = "bus_region"


#DELETE between each transit mode run#
TBL_TRANSIT_NODE = "transit_node"
TBL_NODE_TRANSIT = "node_transit"
TBL_NODENOS = "nodenos_transit"
TBL_NODES_GEOFF = "nodes_geoff_transit"
TBL_NODES_GID = "nodes_gid_transit"
TBL_GEOFF_NODES = "geoff_nodes_transit"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_transit"
TBL_GEOFF_GROUP = "geoff_group_transit"
TBL_GID_NODES = "gid_nodes_transit"
TBL_NODE_GID = "node_gid_post_transit"


IDX_GEOFF_GROUP = "geoff_group_value_idx_transit"
IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx_transit"
IDX_NODENOS = "nodeno_idx_transit"
IDX_NODES_GEOFF = "nodes_geoff_idx_transit"
IDX_NODES_GID = "nodes_gid_idx_transit"
IDX_GEOFF_NODES = "geoff_nodes_idx_transit"
IDX_GID_NODES = "gid_nodes_idx_transit"
IDX_OD_value = "od_value_idx_transit"
IDX_NODE_GID = "node_gid_post_idx_transit"
IDX_TRANSIT_NODE = "transit_node_idx_transit"
IDX_NODE_TRANSIT = "node_transit_idx_transit"

#connect to DB
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()
    
#grab info on geoffs, blocks, and transit intersections
SQL_GetGeoffs = """SELECT geoffid, vianode, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_GEOFF_LOOKUP_GEOM)
SQL_GetBlocks = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_CENTS)

SQL_GettransitInt = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_TRANSIT)

def GetCoords(record):
    id, vianode, geojson = record
    return id, vianode, json.loads(geojson)['coordinates']
def ExecFetchSQL(SQL_Stmt):
    cur = con.cursor()
    cur.execute(SQL_Stmt)
    return map(GetCoords, cur.fetchall())

print time.ctime(), "Getting Blocks"
#create OD list
data = ExecFetchSQL(SQL_GetGeoffs)
world_ids, world_vias, world_coords = zip(*data)
node_coords = dict(zip(world_vias, world_coords))
geoff_nodes = dict(zip(world_ids, world_vias))
# Node to Geoff dictionary (a 'random' geoff will be selected for each node)
nodes_geoff = dict(zip(world_vias, world_ids))
geofftree = scipy.spatial.cKDTree(world_coords)
del world_coords, world_vias

data = ExecFetchSQL(SQL_GetBlocks)
#results is list of block ids and closest nodenos
results = []
for i, (id, _, coord) in enumerate(data):
    dist, index = geofftree.query(coord)
    geoffid = world_ids[index]
    nodeno = geoff_nodes[geoffid]
    results.append((id, nodeno))
    
print time.ctime(), "Getting transits"
transits = ExecFetchSQL(SQL_GettransitInt)
#transit results is list of transit ids and closest nodenos
transit_results = []
for i, (id, _, coord) in enumerate(transits):
    dist, index = geofftree.query(coord)
    geoffid = world_ids[index]
    nodeno = geoff_nodes[geoffid]
    transit_results.append((id, nodeno))

#transit dict is transit results in dictionary form
transit_dict = {}
for tid, node in transit_results:
    if not tid in transit_dict:
        transit_dict[tid] = []
    transit_dict[tid].append(node)
    
del data, transits, world_ids
    
gids, nodenos = zip(*results)
#nodenos = sorted(set(nodenos))
# Node to GID dictionary (a 'random' GID will be selected for each node)
nodes_gids = dict(zip(nodenos, gids))

tid, node = zip(*transit_results)
node_tid = dict(zip(node, tid))

#create gid_node to track which blocks use the same O and D nodes 
#this will be used to determine how many times to count each path later
node_gid = {}
gid_node = {}
for GID, nodeno in results:
    if not nodeno in node_gid:
        node_gid[nodeno] = []
    if GID in gid_node:
        print "Warn %d" % GID
    node_gid[nodeno].append(GID)
    gid_node[GID] = nodeno
    
print time.ctime(), "Writing Intermediate Tables"
#convert nodes_geoff, and nodes_gids dictionaries to list and save to tables in postgres
nodes_geoff_list = [(k, v) for k, v in nodes_geoff.iteritems()]
nodes_gids_list = [(k, v) for k, v in nodes_gids.iteritems()]
geoff_nodes_list = [(k, v) for k, v in geoff_nodes.iteritems()]
gid_node_list = [(k, v) for k, v in gid_node.iteritems()]#added for postprocessing
transit_node_list = [(k, v[0]) for k, v in transit_dict.iteritems()]#added for postprocessing
node_transit_list = [(k, v) for k, v in node_tid.iteritems()]#added for postprocessing

node_gid_list = [] #added for postprocessing
for key, value in node_gid.iteritems():
    for item in value:
        node_gid_list.append((key, item))
        
#write these plus nodesnos list into postgres to call later to turn back into a dictionary
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

#write gid_node into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    gid integer,
    nodes integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (nodes, gid)
    TABLESPACE pg_default;    
""".format(TBL_GID_NODES, IDX_GID_NODES)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(gid_node_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(gid_node_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in gid_node_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_GID_NODES, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

#write gid_node into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    node integer,
    gid integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (node, gid)
    TABLESPACE pg_default;    
""".format(TBL_NODE_GID, IDX_NODE_GID)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(node_gid_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(node_gid_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in node_gid_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_NODE_GID, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

#write transit_dict into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    transitid integer,
    node integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (node, transitid)
    TABLESPACE pg_default;    
""".format(TBL_TRANSIT_NODE, IDX_TRANSIT_NODE)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(transit_node_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(transit_node_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in transit_node_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_TRANSIT_NODE, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")


#write node_transit into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    node integer,
    transitid integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (node, transitid)
    TABLESPACE pg_default;    
""".format(TBL_NODE_TRANSIT, IDX_NODE_TRANSIT)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(node_transit_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(node_transit_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in node_transit_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_NODE_TRANSIT, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

print time.ctime(), "Calculating transit Distance"
#find transit/block centroid pairs within 3 miles SLD, per FTA bike to transit manual
#if tids share a closest node, only one tid is selected for calculating shortest paths. these are accounted for in edge counting.
Q_transitDist = """
WITH tblA AS(
    SELECT DISTINCT ON(t.node) t.node, t.transitid AS tid, g.geom
    FROM "{1}" t
    INNER JOIN "{0}" g
    ON t.node = g.vianode
    )
SELECT t.tid, c.gid
FROM tblA t
INNER JOIN "{2}" c
ON ST_DWithin(t.geom, c.geom, 4828.03)
""".format(TBL_GEOFF_LOOKUP_GEOM,TBL_TRANSIT_NODE, TBL_CENTS)
cur.execute(Q_transitDist)
output = cur.fetchall()
transitpairs = []
for tid, gid in output:
    transitpairs.append((tid, gid))


#create gid_node to track which blocks use the same O and D nodes 
#this will be used to determine how many times to count each path later
node_gid = {}
gid_node = {}
for GID, nodeno in results:
    if not nodeno in node_gid:
        node_gid[nodeno] = []
    if GID in gid_node:
        print "Warn %d" % GID
    node_gid[nodeno].append(GID)
    gid_node[GID] = nodeno


del gids, results

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

#grab list of block centroids states to create a lookup to be referenced later
SQL_GetBlockState = """SELECT gid, statefp10 FROM "{0}";""".format(TBL_ALLCENTS)
cur.execute(SQL_GetBlockState)
states = cur.fetchall()

state_lookup = {}
for gid, state in states:
    if not gid in state_lookup:
        state_lookup[gid] = []
    state_lookup[gid].append(state)
    
#repeat to create lookup for transit states
SQL_GetTransitState = """SELECT gid, statenum FROM "{0}";""".format(TBL_TRANSIT)
cur.execute(SQL_GetTransitState)
transitstates = cur.fetchall()

transit_state_lookup = {}
for tid, state in transitstates:
    if not tid in transit_state_lookup:
        transit_state_lookup[tid] = []
    transit_state_lookup[tid].append(state)

#grab list of block centroid gids that are within 5 miles of a delaware river bridge
Q_BridgeList = """SELECT gid, statefp10 FROM "{0}";""".format(TBL_BRIDGECENTS)
cur.execute(Q_BridgeList)
bridge_list = cur.fetchall()
cent, state = zip(*bridge_list)

print time.ctime(), "Creating OD Pair List"
CloseEnough = []
OutsideBridgeBuffer = 0
DiffGroup = 0
NullGroup = 0
for i, (tid, gid) in enumerate(transitpairs):
        fromnodeno = transit_dict[tid][0]
        tonodeno = gid_node[gid]
        #are the nodes on the same island?
        if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
            #are the from/to nodes of the OD pair the same point?
            if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
                #are they in the same state? if so, calculate
                if transit_state_lookup[tid] == state_lookup[gid]:
                    # if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
                    CloseEnough.append([
                        tid,    # FromGID
                        fromnodeno,                # FromNode
                        nodes_geoff[fromnodeno],   # FromGeoff
                        gid,      # ToGID
                        tonodeno,                  # ToNode
                        nodes_geoff[tonodeno],     # ToGeoff
                        geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                        ])
                    #if not, are both ends in the bridge buffer? if so, calculate
                elif tid and gid in cent:
                    CloseEnough.append([
                        tid,    # FromGID
                        fromnodeno,                # FromNode
                        nodes_geoff[fromnodeno],   # FromGeoff
                        gid,      # ToGID
                        tonodeno,                  # ToNode
                        nodes_geoff[tonodeno],     # ToGeoff
                        geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                        ])
                    #if not, don't calculate
                else:
                    OutsideBridgeBuffer += 1        
            else:
                DiffGroup += 1
        else:
            NullGroup += 1
        
print time.ctime(), "Length of CloseEnough = ", len(CloseEnough)

print time.ctime(), "Writing out OD pair list"
#write out close enough to table
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(

    fromtid     integer,
    fromnode    integer,
    fromgeoff   integer,
    togid       integer,
    tonode      integer,
    togeoff     integer,
    groupnumber integer,
    rowno       BIGSERIAL PRIMARY KEY
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;
CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (fromtid, fromnode, fromgeoff, togid, tonode, togeoff, groupnumber)
    TABLESPACE pg_default;    
""".format(TBL_BLOCK_NODE_GEOFF, IDX_BLOCK_NODE_GEOFF)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(CloseEnough[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(CloseEnough), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in CloseEnough[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_BLOCK_NODE_GEOFF, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

Q_Index = """
    CREATE INDEX block_node_geoff_grp_idx
      ON public.{0}
      USING btree
      (groupnumber);

    COMMIT;
    """.format(TBL_BLOCK_NODE_GEOFF)
cur.execute(Q_Index)


print time.ctime(), "Finished"

