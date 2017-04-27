import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys
import json
import scipy.spatial

TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco_tolerablelinks"
TBL_NODES = "montco_nodes"
TBL_SPATHS = "montco_shortestpaths"
TBL_TOLNODES = "montco_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_geoffs"
TBL_GEOFF_GEOM = "montco_geoffs_viageom"
TBL_MASTERLINKS = "montco_master_links"
TBL_MASTERLINKS_GEO = "montco_master_links_geo"

Q_ShortestPathwTurns = """
    SELECT %d AS sequence, %d AS oGID, %d AS dGID, * FROM pgr_dijkstra(
        'SELECT mixid AS id, fromgeoff AS source, togeoff AS target, cost AS cost FROM "{0}"', 
        %d, %d
    );
""".format(TBL_MASTERLINKS_GEO)

Q_InsertShortestPath = """
    INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""".format(TBL_SPATHS)

SQL_GetGeoffs = """SELECT geoffid, vianode, ST_AsGeoJSON(geom) FROM "montco_geoffs_viageom";"""
SQL_GetBlocks = """SELECT gid, Null AS dummy, ST_AsGeoJSON(geom) FROM "montco_blockcent";"""

def GetCoords(record):
    id, vianode, geojson = record
    return id, vianode, json.loads(geojson)['coordinates']
def ExecFetchSQL(SQL_Stmt):
    cur = con.cursor()
    cur.execute(SQL_Stmt)
    return map(GetCoords, cur.fetchall())

worker_number = int(sys.argv[1])
pool_size = int(sys.argv[2])

start_time = time.time()
print time.ctime(), "Section 1"
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()
data = ExecFetchSQL(SQL_GetGeoffs)
world_ids, world_vias, world_coords = zip(*data)
node_coords = dict(zip(world_vias, world_coords))
geoff_nodes = dict(zip(world_ids, world_vias))
# Node to Geoff dictionary (a 'random' geoff will be selected for each node)
nodes_geoff = dict(zip(world_vias, world_ids))
geofftree = scipy.spatial.cKDTree(world_coords)

print time.ctime(), "Section 2"
data = ExecFetchSQL(SQL_GetBlocks)
results = []
for i, (id, _, coord) in enumerate(data):
    _st = time.time()
    dist, index = geofftree.query(coord)
    geoffid = world_ids[index]
    nodeno = geoff_nodes[geoffid]
    results.append((id, nodeno))

print time.ctime(), "Section 3"
gids, nodenos = zip(*results)
nodenos = sorted(set(nodenos))
# Node to GID dictionary (a 'random' GID will be selected for each node)
nodes_gids = dict(zip(nodenos, gids))
nodetree = scipy.spatial.cKDTree(map(lambda nodeno:node_coords[nodeno], nodenos))
sdm = nodetree.sparse_distance_matrix(nodetree, 8046.72)

print time.ctime(), "Section 4"
OandD = sorted(sdm.keys())

CloseEnough = []
for i, (fromnodeindex, tonodeindex) in enumerate(OandD):
    if i % pool_size == (worker_number - 1):
        fromnodeno = nodenos[fromnodeindex]
        tonodeno = nodenos[tonodeindex]
        CloseEnough.append([
            nodes_gids[fromnodeno],  # FromGID
            nodes_geoff[fromnodeno], # FromGeoff
            nodes_gids[tonodeno],    # ToGID
            nodes_geoff[tonodeno]    # ToGeoff
        ])
print time.ctime(), "Workload :: %d of %d" % (len(CloseEnough), len(OandD))
print time.ctime(), time.time() - start_time

del data, geoff_nodes, gids, node_coords, nodenos, nodes_gids, nodetree, OandD, results, sdm, world_coords, world_ids, world_vias

TooLong = 0
NoPath = 0
runTimes = []
runTimesTooLong = []
runTimesNoPath = []
splits = []

_time0 = time.time()
for i, (oGID, oNode, dGID, dNode) in enumerate(CloseEnough):
    if (i % 1000 == 0):
        print "%s :: %.2f :: %d :: %.2f" % (time.ctime(), time.time() - _time0, i, i / (len(CloseEnough) / 100.0))
        _time0 = time.time()
    #raised threshold to ensure shortest paths with lots of turns are not eliminated
    threshold = 100
    start_time = time.time()
    _time1 = time.time()
    cur.execute(Q_ShortestPathwTurns % (i, oGID, dGID, oNode, dNode))
    # print "\tQuerying :: %.2f" % (time.time() - _time1)

    # _time1 = time.time()
    results = cur.fetchall()
    # print "\tFetching :: %.2f" % (time.time() - _time1)

    # _time1 = time.time()
    if len(results) > 0:
        if results[-1][-1] <= threshold:
            cur.executemany(Q_InsertShortestPath, results)
            con.commit()
            runTimes.append(time.time() - start_time)
        else:
            TooLong += 1
            runTimesTooLong.append(time.time() - start_time)
    else:
        NoPath +=1
        runTimesNoPath.append(time.time() - start_time)
    # print "\tProcessing :: %.2f" % (time.time() - _time1)
    splits.append(time.time() - _time1)

print(TooLong)
print(NoPath)

avg = lambda iterable:sum(iterable)/float(len(iterable))
print min(runTimes), max(runTimes), avg(runTimes), sum(runTimes)

#change file location and slash direction
#endtime = time.ctime()
file = open("%d.txt" % worker_number,"w")
#file.write(endtime)
file.close()
