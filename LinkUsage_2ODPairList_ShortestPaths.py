import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys

TBL_CENTS = "mercer_centroids"
TBL_LINKS = "mercer_tolerablelinks_improved"
TBL_NODES = "mercer_nodes_improved"
TBL_SPATHS = "mercer_shortestpaths_improved"

Q_CreateODList = """
    SELECT
        min(t50.ogid) AS oGID,
        t50.oNode,
        min(t50.dgid) AS dGID,
        t50.dNode
    FROM (
        -- Temporary table saving GID, Node correspondence
        WITH blockNode AS (
            WITH tbl1 AS (
                WITH tbl0 AS (
                    SELECT
                        "{0}".gid AS gid,
                        "{0}".geom AS geom,
                        ST_Buffer("{0}".geom, 500) AS buffer
                    FROM "{0}"
                )
                SELECT 
                    tbl0.gid as gid,
                    "{1}".nodeno AS nodeno,
                    ST_Distance(tbl0.geom, "{1}".geom) AS dist
                FROM 
                    "{1}",
                    tbl0
                WHERE 
                    tbl0.buffer && "{1}".geom
            )
            SELECT
                tbl20.gid,
                tbl1.nodeno
            FROM (
                SELECT
                    gid,
                    MIN(dist) AS dist
                FROM tbl1
                GROUP BY gid
            ) AS tbl20, tbl1
            WHERE tbl20.gid = tbl1.gid
            AND tbl20.dist = tbl1.dist
            GROUP BY tbl20.gid, tbl1.nodeno
        )
        SELECT
            t40.oNode AS oNode,
            t41.gid AS oGID,
            t40.dNode AS dNode,
            t42.gid AS dGID,
            t40.cnt AS cnt
        FROM (
            -- WHERE query for agg. field
            SELECT *
            FROM (
                -- Grouping oGID, dGID by oNode,dNode
                SELECT
                    t20.oNode,
                    t20.dNode,
                    COUNT(*) AS cnt
                FROM (
                    -- SQL OandD
                    SELECT
                        t10.gid1 AS oGID,
                        t11.nodeno AS oNode,
                        t10.gid2 AS dGID,
                        t12.nodeno AS dNode
                    FROM (
                        -- SQL combinations
                        SELECT DISTINCT
                            t1.gid AS gid1,
                            t2.gid AS gid2
                        FROM blockNode AS t1,
                             blockNode AS t2
                    ) AS t10
                    INNER JOIN blockNode AS t11
                    ON t11.gid = t10.gid1
                    INNER JOIN blockNode AS t12
                    ON t12.gid = t10.gid2
                ) AS t20
                WHERE t20.oNode <> t20.dNode
                GROUP BY t20.oNode, t20.dNode
            ) AS t30
        ) AS t40
        INNER JOIN blockNode AS t41
        ON t41.nodeno = t40.oNode
        INNER JOIN blockNode AS t42
        ON t42.nodeno = t40.dNode
        ORDER BY oNode, dNode
    ) AS t50
    GROUP BY t50.onode, t50.dnode;
""".format(TBL_CENTS, TBL_NODES)

Q_StraigtLineDist = """
    SELECT ST_Distance(a.geom, b.geom)
    FROM {0} a, {0} b
    WHERE a.nodeno = %d AND b.nodeno = %d
    GROUP BY a.geom, b.geom;
""".format(TBL_NODES)

Q_ShortestPath = """
    SELECT %d AS sequence, %d AS oGID, %d AS dGID, * FROM pgr_dijkstra(
        'SELECT gid AS id, fromnodeno AS source, tonodeno AS target, (CAST(trim(trailing ''mi'' FROM "length") AS float)* (1 + "linklts")) AS cost FROM "{0}"', 
        %d, %d
    );
""".format(TBL_LINKS)

Q_InsertShortestPath = """
    INSERT INTO {0} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""".format(TBL_SPATHS)

worker_number = int(sys.argv[1])
pool_size = int(sys.argv[2])

con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

start_time = time.time()
cur.execute(Q_CreateODList)
print "Calculating OD combinations :: %.2f" % (time.time() - start_time)
OandD = cur.fetchall()

mywork = []
for i in xrange(len(OandD)):
    if i % pool_size == (worker_number - 1):
        mywork.append(OandD[i])
print "Workload :: %d of %d" % (len(mywork), len(OandD))
del OandD

CloseEnough = []
TooFarApart = 0
DistanceChecked = {}
splits = []
start_time = time.time()
for i, (oGID, oNode, dGID, dNode) in enumerate(mywork):
    _time1 = time.time()
    threshold = 8046.72
    if (oNode, dNode) in DistanceChecked:
        pass
    else:
        cur.execute(Q_StraigtLineDist % (oNode, dNode))
        results = cur.fetchall()
        DistanceChecked[(oNode, dNode)] = None
        DistanceChecked[(dNode, oNode)] = None
        if results[0][0] <= threshold:
            CloseEnough.append(mywork[i])
            CloseEnough.append(mywork[i][::-1])
        else:
            TooFarApart += 1
    splits.append(time.time() - _time1)
print "Filtering OD line distance :: %.2f" % (time.time() - start_time)
del mywork

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

    threshold = 10
    start_time = time.time()
    _time1 = time.time()
    cur.execute(Q_ShortestPath % (i, oGID, dGID, oNode, dNode))
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