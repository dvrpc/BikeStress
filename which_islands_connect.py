-- SELECT * FROM "montco_groups";
SELECT * FROM "montco_L3_linkuse";
-- SELECT * FROM montco_master_links_grp;




SELECT st_concavehull(st_Collect(geom), 0.99)
FROM montco_master_links_grp
WHERE strong = 446;

SELECT st_concavehull(st_Collect(geom), 0.99)
FROM montco_master_links_grp
WHERE strong = 446 OR strong = 250
GROUP BY strong;

-- TESTING ON ONE LINK AND 2 BLOBS
WITH blobs AS(
	SELECT st_concavehull(st_Collect(geom), 0.99)
	FROM montco_master_links_grp
	WHERE strong = 446 OR strong = 250
	GROUP BY strong)

SELECT blobs.*
FROM blobs
INNER JOIN (
	SELECT *
	FROM montco_lts_links
	WHERE gid = 46449) link
ON ST_Intersects(link.geom, blobs.st_concavehull)
;

-- REAL ONE

-- PYTHON?
import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle
import sqlite3
from collections import Counter
import json
import scipy.spatial
import networkx as nx


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()


#select top 10 percent from county lts result tables
TBL_bucks        = "bucks_lts3_linkuse"
TBL_chester      = "chester_lts3_linkuse"
TBL_delaware     = "delaware_lts3_linkuse"
TBL_montgomery   = "montgomery_lts3_linkuse"
TBL_philadelphia = "philadelphia_lts3_linkuse"

county_tbls = (TBL_bucks, TBL_chester, TBL_delaware, TBL_montgomery, TBL_philadelphia)

priority_edges = []

for county in county_tbls:
    cur.execute("""SELECT COUNT(*)/10 AS top10percent FROM public."{0}";""").format(county)
    top = cur.fetchall()
    topint = int(top[0][0])

    cur.execute("""SELECT edge FROM public."{1}" ORDER BY total DESC LIMIT {0};""").format(topint, county)
    results = cur.fetchall()
    for edge in results:
        priority_edges.append(edge[0][0])
        


TBL_LINKS_GRP = "montco_master_links_grp" 
TBL_TOP20PERCENT = "montco_top20percent"
TBL_BLOBS = "montco_grp_blobs"
TBL_CON_ISLANDS = "montco_con_islands2"

Q_CreateBlobs = """
	SELECT strong, st_concavehull(st_Collect(geom), 0.99) geo
	FROM "{0}"
    WHERE strong = {1}
	GROUP BY strong;"""

blobs = []
for i in xrange(1,4074):
    cur.execute(Q_CreateBlobs.format(TBL_LINKS_GRP, i))
    blob = cur.fetchall()
    blobs.append(blob)
    
Q_CreateTable = """
    CREATE TABLE public."{0}"
    (
      strong integer,
      geom geometry(Geometry,26918)
    )
    WITH (
      OIDS=FALSE
    );
    COMMIT;"""
    
cur.execute(Q_CreateTable.format(TBL_BLOBS))

Q_Insert = """INSERT INTO public."{0}" (strong, geom) VALUES ({1},{2});"""

for i in xrange(0,len(blobs)):
    island = blobs[i][0][0]
    outline = blobs[i][0][1]
    cur.execute(Q_Insert.format(TBL_BLOBS, island, outline))
    

    
    
WITH blobs AS(
	SELECT st_concavehull(st_Collect(geom), 0.99) geo
	FROM montco_master_links_grp
    WHERE strong IS NOT NULL
	GROUP BY strong)

SELECT link.edge, link.count, blobs.geo
FROM blobs
INNER JOIN (
	SELECT *
	FROM montco_top20percent) link
ON ST_Intersects(link.geom, blobs.geo)
ORDER BY count, edge
;


-- FOR SUMMARIZING
WITH blobs AS(
	SELECT st_concavehull(st_Collect(geom), 0.99) geo
	FROM montco_master_links_grp
	GROUP BY strong)

SELECT COUNT(*)
FROM(
	SELECT count(*) AS cnt, edge
	FROM(
		SELECT link.edge, link.count, blobs.geo
		FROM blobs
		INNER JOIN (
			SELECT *
			FROM montco_top20percent) link
		ON ST_Intersects(link.geom, blobs.geo)
		ORDER BY count, edge) foo
	GROUP BY edge) goo
;



-- BUFFER METHOD
Q_BUFFER_INTERSECT = """
    WITH buf AS(
        SELECT foo.edge, st_buffer(geom, 10) buffer
            FROM (
                SELECT edge, count, linklts, geom
                FROM "{0}"
                WHERE edge = {1}) foo)

    SELECT 
        DISTINCT(strong),
        goo.edge
    FROM(
        SELECT 
            L.mixid, 
            L.strong, 
            L.geom, 
            B.edge, 
            B.buffer
        FROM "{2}" L
        INNER JOIN (
            SELECT 
                edge,
                buffer
            FROM buf) B
        ON ST_Intersects(L.geom, B.buffer)) goo
    ;"""

cur.execute(Q_BUFFER_INTERSECT.format(TBL_TOP20PERCENT, 46423, TBL_LINKS_GRP))
islands = cur.fetchall()

Q_CreateBlobs = """
	SELECT strong, ST_AsGeoJSON(st_concavehull(st_Collect(geom), 0.99)) geo
	FROM "{0}"
    WHERE strong = {1}
	GROUP BY strong;"""

edgeList = []
islandList = []
blobsGeoms = []
for i in xrange(0, len(islands)):
    if len(islands) == 1:
        print "Only 1 island - Does not connect"
    elif len(islands) >= 2:
        a = islands[i][0]
        edgeList.append(int(islands[i][1]))
        cur.execute(Q_CreateBlobs.format(TBL_LINKS_GRP, a))
        blob = cur.fetchall()
        islandList.append(blob[0][0])
        blobsGeoms.append(blob[0])

Q_CreateTable = """
    CREATE TABLE public."{0}"
    (
      edge integer,
      strong integer,
      geom geometry(Geometry,26918)
    )
    WITH (
      OIDS=FALSE
    );
    COMMIT;"""
    
cur.execute(Q_CreateTable.format(TBL_BLOBS))

Q_Insert = """INSERT INTO public."{0}" (edge, strong, geom) VALUES ({1},{2},(ST_SetSRID(ST_GeomFromGeoJSON('{3}'), 26918)));"""

for i in xrange(0,len(edgeList)):
    edge = edgeList[i]
    island = islandList[i]
    blob = blobsGeoms[i][1]
    cur.execute(Q_Insert.format(TBL_BLOBS, edge, island, blob))



# OR SELECT ALL THE LINKS THAT ARE PART OF THAT ISLAND
Q_BUFFER_INTERSECT = """
    WITH buf AS(
        SELECT foo.edge, st_buffer(geom, 10) buffer
            FROM (
                SELECT edge, count, linklts, geom
                FROM "{0}"
                WHERE edge = {1}) foo)

    SELECT 
        DISTINCT(strong),
        goo.edge
    FROM(
        SELECT 
            L.mixid, 
            L.strong, 
            L.geom, 
            B.edge, 
            B.buffer
        FROM "{2}" L
        INNER JOIN (
            SELECT 
                edge,
                buffer
            FROM buf) B
        ON ST_Intersects(L.geom, B.buffer)) goo
    ;"""

#run this to populate PriorityLinks
Q_PriorityLinks = """
    SELECT edge
    FROM "{0}"
"""


#need to work out dissolve issue
#use these known short links for now
PriorityLinks = [46423, 26615, 55433, 65321, 56887, 58855, 71857]

# islands = []
for link in PriorityLinks:
    cur.execute(Q_BUFFER_INTERSECT.format(TBL_TOP20PERCENT, link, TBL_LINKS_GRP))
    islands = cur.fetchall()
    # for i in xrange(0,len(connected)):
        # islands.append(connected[i])
        
    Q_SELECT_ISLANDS = """
        SELECT mixid, strong, ST_AsGeoJSON(geom)
        FROM "{0}"
        WHERE strong = {1};"""
        
    edgeList = []
    mixidList = []
    islandList = []
    geomList = []
    for i in xrange(0, len(islands)):
        if len(islands) == 1:
            print "Only 1 island - Does not connect"
            #add something to table for these 1 island links
            a = islands[0][0]
            b = int(islands[0][1]
            edgeList.append(b)
            mixidList.append(0)
            islandList.append(a)
            geomList.append(0)
        elif len(islands) >= 2:
            a = islands[i][0]
            b = int(islands[i][1])
            cur.execute(Q_SELECT_ISLANDS.format(TBL_LINKS_GRP, a))
            links = cur.fetchall()
            for j in xrange(0, len(links)):
                edgeList.append(b)
                mixidList.append(links[j][0])
                islandList.append(links[j][1])
                geomList.append(links[j][2])

    Q_CreateTable = """
        CREATE TABLE IF NOT EXISTS public."{0}"
        (
          edge integer,
          mixid integer,
          strong integer,
          geom geometry(Geometry,26918)
        )
        WITH (
          OIDS=FALSE
        );
        COMMIT;"""
        
    cur.execute(Q_CreateTable.format(TBL_CON_ISLANDS))

    #TEST that list lengths match
    if len(edgeList) == len(mixidList) == len(islandList) == len(geomList):
        print "All lists are of equal length"
    else:
        print "List length mismatch"

    Q_Insert = """INSERT INTO public."{0}" (edge, mixid, strong, geom) VALUES ({1},{2},{3}, (ST_SetSRID(ST_GeomFromGeoJSON('{4}'), 26918)));"""

    for i in xrange(0,len(edgeList)):
        edge = edgeList[i]
        mixid = mixidList[i]
        island = islandList[i]
        geo = geomList[i]
        cur.execute(Q_Insert.format(TBL_CON_ISLANDS, edge, mixid, island, geo))

        
#using reslts from above, find total length of connected roads based on the connecting edge
SELECT edge, SUM(ST_Length(geom)) sumLength
FROM montco_con_islands2
GROUP BY edge
ORDER BY sumLength DESC