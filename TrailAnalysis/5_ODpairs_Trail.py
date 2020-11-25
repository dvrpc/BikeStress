#run thru cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\TrailAnalysis\5_ODpairs_Trail.py

import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys
import json
import scipy.spatial
import networkx as nx

from database import connection

#table names
TBL_ALL_LINKS = "links"
TBL_CENTS = "block_centroids"
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

#update tables for each different destination type (trails, schools)
TBL_DEST = "trail_int"
TBL_DEST_NODE = "trail_node"
TBL_NODE_DEST = "node_trail"

TBL_NODENOS = "nodenos_trail"
TBL_NODES_GEOFF = "nodes_geoff_trail"
TBL_NODES_GID = "nodes_gid_trail"
TBL_GEOFF_NODES = "geoff_nodes_trail"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_trail"
TBL_GEOFF_GROUP = "geoff_group_trail"
TBL_GID_NODES = "gid_nodes_trail"
TBL_NODE_GID = "node_gid_post_trail"

IDX_GEOFF_GROUP = "geoff_group_value_idx_trail"
IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx_trail"
IDX_NODENOS = "nodeno_idx_trail"
IDX_NODES_GEOFF = "nodes_geoff_idx_trail"
IDX_NODES_GID = "nodes_gid_idx_trail"
IDX_GEOFF_NODES = "geoff_nodes_idx_trail"
IDX_GID_NODES = "gid_nodes_idx_trail"
IDX_OD_value = "od_value_idx_trail"
IDX_NODE_GID = "node_gid_post_idx_trail"
IDX_DEST_NODE = "trail_node_idx_trail"
IDX_NODE_DEST = "node_trail_idx_trail"

#connect to DB
cur = connection.cursor()
    
#grab info on geoffs, blocks, and destination
SQL_GetGeoffs = """SELECT geoffid, vianode, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_GEOFF_LOOKUP_GEOM)
SQL_GetBlocks = """SELECT gid, Null AS dummy, ST_AsGeoJSON(cent) FROM "{0}";""".format(TBL_CENTS)

SQL_GetDEST = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "{0}";""".format(TBL_DEST)

def GetCoords(record):
    id, vianode, geojson = record
    return id, vianode, json.loads(geojson)['coordinates']
def ExecFetchSQL(SQL_Stmt):
    cur = connection.cursor()
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
    
print time.ctime(), "Getting Destinations"
destinations = ExecFetchSQL(SQL_GetDEST)
#tdest results is list of dest ids and closest nodenos
dest_results = []
for i, (id, _, coord) in enumerate(destinations):
    dist, index = geofftree.query(coord)
    geoffid = world_ids[index[0]]
    nodeno = geoff_nodes[geoffid]
    dest_results.append((id, nodeno))

#dest dict is dest results in dictionary form
#did = destination id
dest_dict = {}
for did, node in dest_results:
    if not did in dest_dict:
        dest_dict[did] = []
    dest_dict[did].append(node)
    
del data, destinations, world_ids
    
gids, nodenos = zip(*results)
#nodenos = sorted(set(nodenos))
# Node to GID dictionary (a 'random' GID will be selected for each node)
nodes_gids = dict(zip(nodenos, gids))

did, node = zip(*dest_results)
node_did = dict(zip(node, did))

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
dest_node_list = [(k, v[0]) for k, v in dest_dict.iteritems()]#added for postprocessing
node_dest_list = [(k, v) for k, v in node_did.iteritems()]#added for postprocessing

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
connection.commit()
            

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

#write dest_dict into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    destid integer,
    node integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (node, destid)
    TABLESPACE pg_default;    
""".format(TBL_DEST_NODE, IDX_DEST_NODE)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(dest_node_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(dest_node_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in dest_node_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_DEST_NODE, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")


#write node_dest into a table in postgres to refer to later
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    node integer,
    destid integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (node, destid)
    TABLESPACE pg_default;    
""".format(TBL_NODE_DEST, IDX_NODE_DEST)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(node_dest_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(node_dest_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in node_dest_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_NODE_DEST, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

print time.ctime(), "Calculating Distance"
#find dest/block centroid pairs within 
#2.5 miles SLD for trails
#2 miles SLD for schools
Q_DestDist = """
    WITH tblA AS(
        SELECT ng.nodes, ng.gid, g.geom
        FROM "{1}" ng
        INNER JOIN "{0}" g
        ON ng.nodes = g.vianode
        ),
    tblB AS(
        SELECT 
            t.gid did,
            a.gid gid, 
            ST_DISTANCE(t.geom, a.geom) dist
        FROM "{2}" t, tblA a
        )
    SELECT *
    FROM tblB
    --2.5 miles
    WHERE dist <= 4023.36
	--2 miles
	--WHERE dist <= 3218.69
""".format(TBL_GEOFF_LOOKUP_GEOM,TBL_NODES_GID, TBL_DEST)
cur.execute(Q_DestDist)
output = cur.fetchall()
destpairs = []
for did, gid, dist in output:
    destpairs.append((did, gid))


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
SQL_GetBlockState = """SELECT gid, statefp10 FROM "{0}";""".format(TBL_CENTS)
cur.execute(SQL_GetBlockState)
states = cur.fetchall()

state_lookup = {}
for gid, state in states:
    if not gid in state_lookup:
        state_lookup[gid] = []
    state_lookup[gid].append(state)
    
#repeat to create lookup for destination states
SQL_GetDestState = """SELECT gid, statenum FROM "{0}";""".format(TBL_DEST)
cur.execute(SQL_GetDestState)
deststates = cur.fetchall()

dest_state_lookup = {}
for did, state in deststates:
    if not did in dest_state_lookup:
        dest_state_lookup[did] = []
    dest_state_lookup[did].append(state)

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
for i, (did, gid) in enumerate(destpairs):
        fromnodeno = dest_dict[did][0]
        tonodeno = gid_node[gid]
        #are the nodes on the same island?
        if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
            #are the from/to nodes of the OD pair the same point?
            if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
                #are they in the same state? if so, calculate
                if dest_state_lookup[did] == state_lookup[gid]:
                    # if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
                    CloseEnough.append([
                        did,    # FromGID
                        fromnodeno,                # FromNode
                        nodes_geoff[fromnodeno],   # FromGeoff
                        gid,      # ToGID
                        tonodeno,                  # ToNode
                        nodes_geoff[tonodeno],     # ToGeoff
                        geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                        ])
                    #if not, are both ends in the bridge buffer? if so, calculate
                elif did and gid in cent:
                    CloseEnough.append([
                        did,    # FromGID
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

    fromdid     integer,
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
    (fromdid, fromnode, fromgeoff, togid, tonode, togeoff, groupnumber)
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

