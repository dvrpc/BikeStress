# Moving Frame Vertical Lines

import logging
import csv
import itertools
import numpy
import time
import sys
import pickle
import cPickle
import sqlite3
from collections import Counter
import json
import scipy.spatial
import networkx as nx
import math
import psycopg2 as psql

TBL_GEOFF_GEOM = "geoffs_viageom"
TBL_MASTERLINKS_GROUPS = "master_links_grp"
TBL_GROUPS = "groups"
TBL_GEOFF_PAIRS = "338_geoff_pairs"
TBL_OD_LINES = "338_OD_lines"
TBL_NODENOS = "nodenos"
# TBL_OD = "OandD"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_NODES_GID = "nodes_gid"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_GEOFF_GEOM = "geoffs_viageom"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
TBL_GEOFF_GROUP = "geoff_group"

VIEW = "links_l3_grp_338"

###set this up like parent/child to iterate through all the existing temp networks
#need to incorporate chunk id here and lower
TBL_TEMP_NETWORK = "temp_network_338_6"
#% chunk_id
TBL_TEMP_PAIRS = "temp_pairs_338_6"
#% chunk_id

con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#
# Q_ODLines = """
    # CREATE TABLE "{0}" AS(
        # WITH unique_geoms AS (
            # SELECT
                # geoffid,
                # geom
            # FROM "{2}"
            # GROUP BY geoffid, geom
        # )
        # SELECT *
        # FROM(
            # SELECT
                # (row_number() over())::bigint AS id,
                # p.fromgeoff AS fromgeoff,
                # p.togeoff AS togeoff,
                # ST_MakeLine(g1.geom, g2.geom) AS geom
            # FROM "{1}" as p
            # INNER JOIN unique_geoms AS g1
            # ON p.fromgeoff = g1.geoffid
            # INNER JOIN unique_geoms AS g2
            # ON p.togeoff = g2.geoffid
        # ) AS pair_f
    # );
    # CREATE SEQUENCE "{0}_id_seq2";
    # SELECT setval(
        # '"{0}_id_seq2"',
        # (
            # SELECT id
            # FROM "{0}"
            # ORDER BY 1 DESC
            # LIMIT 1
        # )
    # );
    # ALTER TABLE "{0}" ALTER COLUMN id SET NOT NULL;
    # ALTER TABLE "{0}" ALTER COLUMN id SET DEFAULT nextval('"{0}_id_seq2"'::regclass);
    # ALTER TABLE "{0}" ADD CONSTRAINT "{0}_pk" PRIMARY KEY (id);
# """.format(TBL_TEMP_PAIRS, TBL_TEMP_PAIRS, TBL_GEOFF_GEOM)
# cur.execute(Q_ODLines)
# con.commit()

#select OD lines that intersect the break line
Q_IntersectLines = """
    SELECT
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom && ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
    """.format(TBL_TEMP_PAIRS)
    
Q_LinesBetween = """
    SELECT 
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom |>> ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918)
        AND geom <<| ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
    """.format(TBL_TEMP_PAIRS)


#use 1 mile buffer when clipping network to capture full paths
Q_ClipNetwork = """
    SELECT 
        mixid,
        fromgeoff,
        togeoff,
        cost,
        ST_AsGeoJSON(geom),
        strong
    FROM public."{0}"
    WHERE geom |&> ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918)
        AND geom &<| ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
""".format(VIEW)

Q_BBoxExtent =   """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";"""

#create table in DB to hold subset of pairs
Q_CreateTempODLinesTable = """
    CREATE TABLE public."{0}"
    (
      id BIGSERIAL PRIMARY KEY,
      fromgeoff integer,
      togeoff integer,
      geom geometry
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public."{0}"
      OWNER TO postgres;"""


Q_InsertTempPairs = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES (%s, %s, ST_GeomFromGeoJSON('%s'));"""

Q_CreateTempNetwork = """
    CREATE TABLE public."{0}"
    (
      id BIGSERIAL PRIMARY KEY, 
      mixid integer,
      fromgeoff integer,
      togeoff integer,
      cost float,
      geom geometry,
      strong integer
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public."{0}"
      OWNER TO postgres;""" 
      
Q_InsertTempNetwork = """INSERT INTO "{0}" (mixid, fromgeoff, togeoff, cost, geom) VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON('%s'));"""


#find extents of bounding box around island
Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(TBL_TEMP_NETWORK)
cur.execute(Q_ExtentCoords)
bbox_json = cur.fetchall()
bbox = json.loads(bbox_json[0][0])
xmin = min(zip(*bbox['coordinates'][0])[0])
xmax = max(zip(*bbox['coordinates'][0])[0])
ymin = min(zip(*bbox['coordinates'][0])[1])
ymax = max(zip(*bbox['coordinates'][0])[1])

iterations = int(math.ceil((xmax-xmin)/8046.72))

#OD LINES IN BETWEEN BREAK LINES

#starting y value of line
x_value_left = xmin
x_value_right = x_value_left + 8046.72
#newid starting at 100 is for between sections
newid = 101
#loop over break lines selecting OD lines that are between them
for i in xrange(1, iterations+1):

    print newid

    TBL_TEMP_PAIRS_2 = "temp_pairs_338_6_%d" % newid
    TBL_TEMP_NETWORK_2 = "temp_network_338_6_%d" % newid
    cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
    con.commit()
    cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
    con.commit()
    
    cur.execute(Q_LinesBetween % (
        x_value_left,
        ymin,
        x_value_left,
        ymax,
        x_value_right,
        ymin,
        x_value_right,
        ymax
    ))
    
    between_pairs = cur.fetchall()
    
    print "Inserting Pairs"
    print len(between_pairs)
    
    str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(between_pairs), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in between_pairs[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
        cur.execute(Q_Insert)
    con.commit()
    
    # for fg, tg, geom in between_pairs:
        # cur.execute(Q_InsertTempPairs.format(TBL_TEMP_PAIRS) % (fg, tg, geom))
    
    print "Clipping Network"    
    
    #clip network with 1 mile buffer on top and bottom      
    cur.execute(Q_ClipNetwork % (
        (x_value_left - 1609.34),
        ymin,
        (x_value_left - 1609.34),
        ymax,
        (x_value_right + 1609.34),
        ymin,
        (x_value_right + 1609.34),
        ymax
    ))
    
    clip_network = cur.fetchall()
    
    print "Inserting Network"
    print len(clip_network)
    
    str_rpl = "(%s, %s, %s, %s, ST_GeomFromGeoJSON('%s'), %s)"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(clip_network), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in clip_network[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO "{0}" (mixid, fromgeoff, togeoff, cost, geom, strong) VALUES {1};""".format(TBL_TEMP_NETWORK_2, arg_str)
        cur.execute(Q_Insert)
    con.commit()
    
    # for mixid, fg, tg, cost, geom, strong in clip_network:
        # cur.execute(Q_InsertTempNetwork.format(TBL_TEMP_NETWORK) % (mixid, fg, tg, cost, geom))
    
    print "Updating SRID"
    
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
    
    x_value_left  += 8046.72
    x_value_right     += 8046.72
    newid        += 1
    

    
#OD LINES THAT INTERSECT BREAK LINES

#starting y value of line
#for intersecting OD lines, start at the second line
x_value = xmin + 8046.72
#newid starting at 1 is for intersection/overlap sections
newid = 1
#loop over break lines selecting OD lines that intersect them
for i in xrange(1,iterations):
    
    print newid
    
    TBL_TEMP_PAIRS_2 = "temp_pairs_338_6_%d" % newid
    TBL_TEMP_NETWORK_2 = "temp_network_338_6_%d" % newid
    cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
    con.commit()
    cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
    con.commit()
    
    cur.execute(Q_IntersectLines % (x_value, ymin, x_value, ymax))
    intersect_pairs = cur.fetchall()
    
    print "Inserting Pairs"
    print len(intersect_pairs)
    
    str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(intersect_pairs), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in intersect_pairs[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
        cur.execute(Q_Insert)
    con.commit()
    
    
    # for fg, tg, geom in intersect_pairs:
        # cur.execute(Q_InsertTempPairs.format(TBL_TEMP_PAIRS) % (fg, tg, geom))
    # con.commit()
    
    print "Creating Extent"
    
    # bounding box around OD lines that intersect the line with 1 mile buffer
    cur.execute(Q_BBoxExtent.format(TBL_TEMP_PAIRS_2))
    intersect_bbox_json = cur.fetchall()
    intersect_bbox = json.loads(intersect_bbox_json[0][0])
    inter_xmin = min(zip(*intersect_bbox['coordinates'][0])[0])
    inter_xmax = max(zip(*intersect_bbox['coordinates'][0])[0])
    inter_ymin = min(zip(*intersect_bbox['coordinates'][0])[1])
    inter_ymax = max(zip(*intersect_bbox['coordinates'][0])[1])
    
    print "Clipping Network"
    
    cur.execute(Q_ClipNetwork % (
        (inter_xmin - 1609.34),
        inter_ymin, 
        (inter_xmin - 1609.34), 
        inter_ymax, 
        (inter_xmax + 1609.34), 
        inter_ymin
        (inter_xmax + 1609.34), 
        inter_ymax
    ))
    
    clip_network = cur.fetchall()
    
    print "Inserting Network"
    print len(clip_network)
    
    # str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(clip_network[0]))))
    str_rpl = "(%s, %s, %s, %s, ST_GeomFromGeoJSON('%s'), %s)"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(clip_network), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in clip_network[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO "{0}" (mixid, fromgeoff, togeoff, cost, geom, strong) VALUES {1};""".format(TBL_TEMP_NETWORK_2, arg_str)
        cur.execute(Q_Insert)
    con.commit()
    
    # for mixid, fg, tg, cost, geom, strong in clip_network:
        # cur.execute(Q_InsertTempNetwork.format(TBL_TEMP_NETWORK) % (mixid, fg, tg, cost, geom))
    # con.commit()
    
    print "Updating SRID"
    
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
    
    #update values for next iteration
    x_value  += 8046.72
    newid    += 1