###RUN PIECE BY PIECE###

import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle



####table names to modify in subsequent runs###
#pa_blockcentroids/ delco_blockcentroids / mercer_centroids
#tolerable_links/ delco_tolerablelinks / mercer_tolerablelinks
#nodes / delco_nodes / mercer_nodes
#shortest_paths_delco
#delco_ODpairs / mercer_ODpairs


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#create list of combinations of closest nodes; does not include self to self
#results in unique node-node pairs with an example O and D GID; ensures Onode <> Dnode
#ensure that when this list is split up, there are no duplicates
#calculates closest node to block centroid from within this querey
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
                        "mercer_centroids".gid AS gid,
                        "mercer_centroids".geom AS geom,
                        ST_Buffer("mercer_centroids".geom, 500) AS buffer
                    FROM "mercer_centroids"
                )
                SELECT 
                    tbl0.gid as gid,
                    "mercer_nodes".nodeno AS nodeno,
                    ST_Distance(tbl0.geom, "mercer_nodes".geom) AS dist
                FROM 
                    "mercer_nodes",
                    tbl0
                WHERE 
                    tbl0.buffer && "mercer_nodes".geom
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
"""
start_time = time.time()
cur.execute(Q_CreateODList)
runTime = (time.time() - start_time)
print runTime
OandD = cur.fetchall()
print len(OandD)


#setup for batch - break into groups to process simultaneously
#batch file will read these in: path [script] [arg] [arg]
#first argument will be the worker number
#second arguement will be the poll size
#these two lines make python aware of the impenting split
worker_number = int(sys.argv[1])
pool_size = int(sys.argv[2])

#split OandD pairs into group to be run
#create list to hold pairs to be worked on
mywork = []
#for each pair in OandD, use the input pool size and worker number from the batch file to choose pairs to be added to mywork
for i in xrange(len(OandD)):
    if i % pool_size == (worker_number - 1):
        mywork.append(OandD[i])
print len(mywork)
del OandD

#query to calculate straight line distance between two points
Q_StraigtLineDist = """
    SELECT ST_Distance(a.geom, b.geom)
    FROM mercer_nodes a, mercer_nodes b
    WHERE a.nodeno = %d AND b.nodeno = %d
    GROUP BY a.geom, b.geom;
"""


#create place to hold OD pairs that will be used in shortest path calculation
CloseEnough = []
#counter to see how many are too far to calculate
TooFarApart = 0
#create place to hold OD pairs where SLD has been calculated
DistanceChecked = {}
#calculate straight line distance between OD pairs
#if SLD is <= 5 miles (in meters - based on projection units), add it to list that will be run through shortest path search
start_time = time.time()
for i, (oGID, oNode, dGID, dNode) in enumerate(mywork):
    threshold = 8046.72
    if (oNode, dNode) in DistanceChecked:
    # Already calculated, skipping...
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
runTime = (time.time() - start_time)
print runTime
print len(CloseEnough)
#numbers in DistanceChecked and CloseEnough will not add up easily and its OK
#some block centroids have same closest node and this accounts for most of the missing pairs in DistanceChecked

#query to find shortest path; one to one
#link cost is calculated on the fly by multiplying link length by (1+LinkLTS)
#make sure all trails/off road bike facilities have LinkLTS of 0
Q_ShortestPath = """
    SELECT %d AS sequence, %d AS oGID, %d AS dGID, * FROM pgr_dijkstra(
        'SELECT gid AS id, fromnodeno AS source, tonodeno AS target, (CAST(trim(trailing ''mi'' FROM "length") AS float)* (1 + "linklts")) AS cost FROM "mercer_tolerablelinks"', 
        %d, %d
    );
"""

#query to insert the resulting values of the shortest path search (6 columns for output values plus additional values for sequence counter and weights)
Q_InsertShortestPath = """
    INSERT INTO mercer_shortestpaths VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""


#count number of paths created that are too long to be added to table and number of paths that are not connected 
#create counter
TooLong = 0
NoPath = 0

runTimes = []
runTimesTooLong = []
runTimesNoPath = []

for i, (oGID, oNode, dGID, dNode) in enumerate(CloseEnough):
    start_time = time.time()
    threshold = 10
    cur.execute(Q_ShortestPath % (i, oGID, dGID, oNode, dNode))
    results = cur.fetchall()
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


print(TooLong)
print(NoPath)

avg = lambda iterable:sum(iterable)/float(len(iterable))
print min(runTimes), max(runTimes), avg(runTimes), sum(runTimes)






#below is for slow method of pinging DB for next unprocessed pair

##create table to hold OD pairs to be searched
#Q_CreateODPairTable = """
#    CREATE TABLE IF NOT EXISTS public.mercer_ODpairs
#    (
#        ogid integer,
#        oNode integer,
#        dgid integer,
#        dNode integer,
#        status integer
#    )
#    WITH (
#        OIDS = FALSE
#    )
#    TABLESPACE pg_default;
#"""
#cur.execute(Q_CreateODPairTable)
#con.commit()
#
##insert value into that edge count table
#Q_InsertODPairs = """
#    INSERT INTO mercer_ODpairs VALUES (%s, %s, %s, %s);
#"""
#cur.executemany(Q_InsertODPairs, CloseEnough)
#con.commit()
#
##query to update status to 1 (as a starting point)
##Status = 1: shortest path not calculated
##Status = 2: shortest path calcualtion in progress
##Status = 3: shortest path calculation complete
#Q_SetStatus = """
#    UPDATE mercer_ODpairs SET status = 1;
#    COMMIT;
#"""
#cur.execute(Q_SetStatus)
#
##when testing script 3, use this to reset that status column
#UPDATE mercer_odpairs SET status = 1 WHERE status <> 1;
#COMMIT;
#-- SELECT * FROM mercer_odpairs WHERE status <> 1;

