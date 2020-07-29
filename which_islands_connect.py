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

-- PYTHON
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


#select top 10 percent from county lts result tables to identify priority links to work with
TBL_bucks        = "bucks_lts3_linkuse"
TBL_chester      = "chesco_lts3_linkuse"
TBL_delaware     = "delco_lts3_linkuse"
TBL_montgomery   = "montco_lts3_linkuse"
TBL_philadelphia = "phila_lts3_linkuse"

county_tbls = (TBL_bucks, TBL_chester, TBL_delaware, TBL_montgomery, TBL_philadelphia)
county_labels = ("Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia")

#create list of priority edges to find connecting islands for
priority_edges = []
edge_county = []

for i in xrange(0, len(county_tbls)):
    cur.execute("""SELECT COUNT(*)/10 AS top10percent FROM public."{0}";""".format(county_tbls[i]))
    top = cur.fetchall()
    topint = int(top[0][0])
    print topint

    cur.execute("""SELECT edge FROM public."{1}" ORDER BY total DESC LIMIT {0};""".format(topint, county_tbls[i]))
    results = cur.fetchall()
    
    for edge in results:
        priority_edges.append(edge[0])
        edge_county.append(county_labels[i])
        
priorities = zip(priority_edges, edge_county)
        


TBL_LINKS_GRP = "master_links_grp_L2" 
TBL_CON_ISLANDS = "connected_islands"




# Q_CreateBlobs = """
	# SELECT strong, st_concavehull(st_Collect(geom), 0.99) geo
	# FROM "{0}"
    # WHERE strong = {1}
	# GROUP BY strong;"""

# blobs = []
# for i in xrange(1,4074):
    # cur.execute(Q_CreateBlobs.format(TBL_LINKS_GRP, i))
    # blob = cur.fetchall()
    # blobs.append(blob)
    
# Q_CreateTable = """
    # CREATE TABLE public."{0}"
    # (
      # strong integer,
      # geom geometry(Geometry,26918)
    # )
    # WITH (
      # OIDS=FALSE
    # );
    # COMMIT;"""
    
# cur.execute(Q_CreateTable.format(TBL_BLOBS))

# Q_Insert = """INSERT INTO public."{0}" (strong, geom) VALUES ({1},{2});"""

# for i in xrange(0,len(blobs)):
    # island = blobs[i][0][0]
    # outline = blobs[i][0][1]
    # cur.execute(Q_Insert.format(TBL_BLOBS, island, outline))
    

    
    
# WITH blobs AS(
	# SELECT st_concavehull(st_Collect(geom), 0.99) geo
	# FROM montco_master_links_grp
    # WHERE strong IS NOT NULL
	# GROUP BY strong)

# SELECT link.edge, link.count, blobs.geo
# FROM blobs
# INNER JOIN (
	# SELECT *
	# FROM montco_top20percent) link
# ON ST_Intersects(link.geom, blobs.geo)
# ORDER BY count, edge
# ;


# -- FOR SUMMARIZING
# WITH blobs AS(
	# SELECT st_concavehull(st_Collect(geom), 0.99) geo
	# FROM montco_master_links_grp
	# GROUP BY strong)

# SELECT COUNT(*)
# FROM(
	# SELECT count(*) AS cnt, edge
	# FROM(
		# SELECT link.edge, link.count, blobs.geo
		# FROM blobs
		# INNER JOIN (
			# SELECT *
			# FROM montco_top20percent) link
		# ON ST_Intersects(link.geom, blobs.geo)
		# ORDER BY count, edge) foo
	# GROUP BY edge) goo
# ;



# BUFFER METHOD
# Q_BUFFER_INTERSECT = """
    # WITH buf AS(
        # SELECT foo.edge, st_buffer(geom, 10) buffer
            # FROM (
                # SELECT edge, total, linklts, geom
                # FROM "{0}"
                # WHERE edge = {1}) foo)

    # SELECT 
        # DISTINCT(strong),
        # goo.edge
    # FROM(
        # SELECT 
            # L.mixid, 
            # L.strong, 
            # L.geom, 
            # B.edge, 
            # B.buffer
        # FROM "{2}" L
        # INNER JOIN (
            # SELECT 
                # edge,
                # buffer
            # FROM buf) B
        # ON ST_Intersects(L.geom, B.buffer)) goo
    # ;"""

# for county in county_tbls:
    # for edge in priority_edges:
        # cur.execute(Q_BUFFER_INTERSECT.format(county, edge, TBL_LINKS_GRP))
        # islands = cur.fetchall()

# Q_CreateBlobs = """
	# SELECT strong, ST_AsGeoJSON(st_concavehull(st_Collect(geom), 0.99)) geo
	# FROM "{0}"
    # WHERE strong = {1}
	# GROUP BY strong;"""

# edgeList = []
# islandList = []
# blobsGeoms = []
# for i in xrange(0, len(islands)):
    # if len(islands) == 1:
        # print "Only 1 island - Does not connect"
    # elif len(islands) >= 2:
        # a = islands[i][0]
        # edgeList.append(int(islands[i][1]))
        # cur.execute(Q_CreateBlobs.format(TBL_LINKS_GRP, a))
        # blob = cur.fetchall()
        # islandList.append(blob[0][0])
        # blobsGeoms.append(blob[0])

# Q_CreateTable = """
    # CREATE TABLE public."{0}"
    # (
      # edge integer,
      # strong integer,
      # geom geometry(Geometry,26918)
    # )
    # WITH (
      # OIDS=FALSE
    # );
    # COMMIT;"""
    
# cur.execute(Q_CreateTable.format(TBL_BLOBS))

# Q_Insert = """INSERT INTO public."{0}" (edge, strong, geom) VALUES ({1},{2},(ST_SetSRID(ST_GeomFromGeoJSON('{3}'), 26918)));"""

# for i in xrange(0,len(edgeList)):
    # edge = edgeList[i]
    # island = islandList[i]
    # blob = blobsGeoms[i][1]
    # cur.execute(Q_Insert.format(TBL_BLOBS, edge, island, blob))


    
#create table of 
TBL_LINKS_GRP = "master_links_grp_L2" 

TBL_CON_ISLANDS = "connected_islands"
    
# OR SELECT ALL THE LINKS THAT ARE PART OF THAT ISLAND
Q_BUFFER_INTERSECT = """
    WITH buf AS(
        SELECT foo.edge, st_buffer(geom, 10) buffer
            FROM (
                SELECT edge, total, linklts, geom
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



for edge, county in priorities:
    if county == "Bucks":
        tbl = TBL_bucks
    elif county == "Chester":
        tbl = TBL_chester
    elif county == "Delaware":
        tbl = TBL_delaware
    elif county == "Montgomery":
        tbl = TBL_montgomery
    elif county == "Philadelphia":
        tbl = TBL_philadelphia
    
    print edge, tbl

    cur.execute(Q_BUFFER_INTERSECT.format(tbl, edge, TBL_LINKS_GRP))
    strong = cur.fetchall()
    
    Q_SELECT_ISLANDS = """
        SELECT mixid, strong, ST_AsGeoJSON(geom)
        FROM "{0}"
        WHERE strong = {1};"""
                
    edgeList = []
    mixidList = []
    islandList = []
    geomList = []
    countyList = []
    for i in xrange(0, len(strong)):
        if len(strong) == 1:
            print "Only 1 island - Does not connect"
            #add something to table for these 1 island links
            a = strong[0][0]
            b = int(strong[0][1])
            edgeList.append(b)
            mixidList.append(0)
            islandList.append(a)
            geomList.append(0)
            countyList.append(county)
        elif len(strong) >= 2:
            a = strong[i][0]
            b = int(strong[i][1])
            cur.execute(Q_SELECT_ISLANDS.format(TBL_LINKS_GRP, a))
            links = cur.fetchall()
            for j in xrange(0, len(links)):
                edgeList.append(b)
                mixidList.append(links[j][0])
                islandList.append(links[j][1])
                geomList.append(links[j][2])
                countyList.append(county)

    Q_CreateTable = """
        CREATE TABLE IF NOT EXISTS public."{0}"
        (
          edge integer,
          mixid integer,
          island integer,
          counties varchar(50),
          geom geometry(Geometry,26918)
        )
        WITH (
          OIDS=FALSE
        );
        COMMIT;"""
        
    cur.execute(Q_CreateTable.format(TBL_CON_ISLANDS))

    #TEST that list lengths match
    if len(edgeList) == len(mixidList) == len(islandList) == len(geomList) == len(countyList):
        print "All lists are of equal length"
    else:
        print "List length mismatch"

    #geometry field will be NULL for rows where the edge does not connect multiple islands
    Q_Insert = """INSERT INTO public."{0}" (edge, mixid, island, counties, geom) VALUES ({1},{2},{3},'{4}',(ST_SetSRID(ST_GeomFromGeoJSON('{5}'), 26918)));"""
    Q_Insert_noGeom = """INSERT INTO public."{0}" (edge, mixid, island, counties) VALUES ({1},{2},{3},'{4}');"""

    for i in xrange(0,len(edgeList)):
        edge = edgeList[i]
        mixid = mixidList[i]
        island = islandList[i]
        geo = geomList[i]
        co = countyList[i]
        if geo == 0:
            cur.execute(Q_Insert_noGeom.format(TBL_CON_ISLANDS, edge, mixid, island, co))
        else:
            cur.execute(Q_Insert.format(TBL_CON_ISLANDS, edge, mixid, island, co, geo))

###look at results in table and figure out what to do next...
            
            
            
#using reslts from above, find total length of connected roads based on the connecting edge
SELECT edge, SUM(ST_Length(geom)) sumLength
FROM montco_con_islands2
GROUP BY edge
ORDER BY sumLength DESC