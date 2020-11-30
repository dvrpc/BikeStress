#to run thru cmd (don't have to)
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\6b_MovingFrame.py


#only run for largest island
import logging
import csv
import itertools
import math
import psycopg2 as psql
# logger = multiprocessing.log_to_stderr(logging.INFO)

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

from database import connection


TBL_GEOFF_GEOM = "geoffs_viageom"
TBL_MASTERLINKS_GROUPS =  "master_links_grp"

TBL_GROUPS = "groups"
TBL_GEOFF_PAIRS = "502_geoff_pairs"
TBL_OD_LINES = "502_OD_lines"
TBL_NODENOS = "nodenos"
# TBL_OD = "OandD"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_NODES_GID = "nodes_gid"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
TBL_GEOFF_GROUP = "geoff_group"


cur = connection.cursor()


#select query to create what used to be Views of each island individually
selectisland = """(SELECT * FROM {0} WHERE strong = 502)""".format(TBL_MASTERLINKS_GROUPS)

######################## PART 2 ###########################
####################VERTICAL LINES#########################
print "Part 2 Vertical Lines"
#select OD lines that intersect the break line
Q_IntersectLines = """
    SELECT
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom && ST_SetSRID(ST_MakeLine(ST_Point(%f, %f),ST_Point(%f, %f)), 26918);
    """
    
Q_LinesBetween = """
    SELECT 
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom >> ST_SetSRID(ST_MakeLine(ST_Point(%f, %f),ST_Point(%f, %f)), 26918)
        AND geom << ST_SetSRID(ST_MakeLine(ST_Point(%f, %f),ST_Point(%f, %f)), 26918);
    """


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
    WHERE geom &> ST_SetSRID(ST_MakeLine(ST_Point(%d, %f),ST_Point(%d, %f)), 26918)
        AND geom &< ST_SetSRID(ST_MakeLine(ST_Point(%d, %f),ST_Point(%d, %f)), 26918);
"""

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

print "Vertical Lines - Intersect/Intersect"

TBL_TEMP_NETWORK = "temp_network_502_%d"
TBL_TEMP_PAIRS = "temp_pairs_502_%d"

#INTERSECT/INTERSECT
for c in xrange(1,50):
    TBL_NETWORK = TBL_TEMP_NETWORK % c
    TBL_PAIRS = TBL_TEMP_PAIRS % c
    
    #find extents of bounding box around island
    Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(TBL_NETWORK)
    cur.execute(Q_ExtentCoords)
    bbox_json = cur.fetchall()
    bbox = json.loads(bbox_json[0][0])
    xmin = min(zip(*bbox['coordinates'][0])[0])
    xmax = max(zip(*bbox['coordinates'][0])[0])
    ymin = min(zip(*bbox['coordinates'][0])[1])
    ymax = max(zip(*bbox['coordinates'][0])[1])

    iterations = int(math.ceil((xmax-xmin)/4828.03))
    #OD LINES THAT INTERSECT BREAK LINES

    #starting y value of line
    #for intersecting OD lines, start at the second line
    x_value = xmin + 4828.03
    #newid starting at 1 is for intersection/overlap sections
    newid = 1
    #loop over break lines selecting OD lines that intersect them
    for z in xrange(1,iterations):
        
        print c, newid
        
        TBL_TEMP_PAIRS_2 = "temp_pairs_502_%d_%d" % (c, newid)
        TBL_TEMP_NETWORK_2 = "temp_network_502_%d_%d" % (c, newid)

        
        cur.execute(Q_IntersectLines.format(TBL_PAIRS) % (x_value, ymin, x_value, ymax))
        intersect_pairs = cur.fetchall()
        
        print "Inserting Pairs"
        print len(intersect_pairs)
        
        if len(intersect_pairs) > 0:
            cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
            connection.commit()
            cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
            connection.commit()
            
            str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
            cur.execute("""BEGIN TRANSACTION;""")
            batch_size = 10000
            for i in xrange(0, len(intersect_pairs), batch_size):
                j = i + batch_size
                arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in intersect_pairs[i:j])
                #print arg_str
                Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
                cur.execute(Q_Insert)
            connection.commit()
            
            print "Creating Extent"
            
            Q_CountPairs = ("""SELECT COUNT(*) FROM "{0}";""").format(TBL_TEMP_PAIRS_2)
            cur.execute(Q_CountPairs)
            NumPairs = cur.fetchall()
            
            #check to see if there are pairs to put a bounding box around
            if int(NumPairs[0][0]) > 0:
                # bounding box around OD lines that intersect the line with 1 mile buffer
                cur.execute(Q_BBoxExtent.format(TBL_TEMP_PAIRS_2))
                intersect_bbox_json = cur.fetchall()
                intersect_bbox = json.loads(intersect_bbox_json[0][0])
                inter_xmin = min(zip(*intersect_bbox['coordinates'][0])[0])
                inter_xmax = max(zip(*intersect_bbox['coordinates'][0])[0])
                inter_ymin = min(zip(*intersect_bbox['coordinates'][0])[1])
                inter_ymax = max(zip(*intersect_bbox['coordinates'][0])[1])
                
                print "Clipping Network"
                
                cur.execute(Q_ClipNetwork.format(TBL_NETWORK) % (
                    (inter_xmin - 1609.34),
                    inter_ymin, 
                    (inter_xmin - 1609.34), 
                    inter_ymax, 
                    (inter_xmax + 1609.34), 
                    inter_ymin,
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
                connection.commit()
                
                print "Updating SRID"
                
                cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
                cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
        
        #if not, drop the tables for that chunk
        else: 
            print "No Pairs in chunk"
            # cur.execute("""DROP TABLE public."{0}";""").format(TBL_TEMP_PAIRS_2)
            # cur.execute("""DROP TABLE public."{0}";""").format(TBL_TEMP_NETWORK_2)

        
        #update values for next iteration
        x_value  += 4828.03
        newid    += 1

#INTERSECT/BETWEEN
print "Vertical Lines - Intersect/Between"
for c in xrange(1,50):
    TBL_NETWORK = TBL_TEMP_NETWORK % c
    TBL_PAIRS = TBL_TEMP_PAIRS % c

    #find extents of bounding box around island
    Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(TBL_NETWORK)
    cur.execute(Q_ExtentCoords)
    bbox_json = cur.fetchall()
    bbox = json.loads(bbox_json[0][0])
    xmin = min(zip(*bbox['coordinates'][0])[0])
    xmax = max(zip(*bbox['coordinates'][0])[0])
    ymin = min(zip(*bbox['coordinates'][0])[1])
    ymax = max(zip(*bbox['coordinates'][0])[1])

    iterations = int(math.ceil((xmax-xmin)/4828.03))

    #OD LINES IN BETWEEN BREAK LINES

    #starting y value of line
    x_value_left = xmin
    x_value_right = x_value_left + 4828.03
    #newid starting at 100 is for between sections
    newid = 101
    #loop over break lines selecting OD lines that are between them
    for z in xrange(1, iterations+1):

        print c ,newid

        TBL_TEMP_PAIRS_2 = "temp_pairs_502_%d_%d" % (c, newid)
        TBL_TEMP_NETWORK_2 = "temp_network_502_%d_%d" % (c, newid)

        
        cur.execute(Q_LinesBetween.format(TBL_PAIRS) % (
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
        
        if len(between_pairs) > 0:
        
            cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
            connection.commit()
            cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
            connection.commit()

            str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
            cur.execute("""BEGIN TRANSACTION;""")
            batch_size = 10000
            for i in xrange(0, len(between_pairs), batch_size):
                j = i + batch_size
                arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in between_pairs[i:j])
                #print arg_str
                Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
                cur.execute(Q_Insert)
            connection.commit()
            
            print "Clipping Network"    
            
            #clip network with 1 mile buffer on top and bottom      
            cur.execute(Q_ClipNetwork.format(TBL_NETWORK) % (
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
            connection.commit()
            
            print "Updating SRID"
            
            cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
            cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
            
        else:
            print "No pairs in chunk"

        x_value_left  += 4828.03
        x_value_right     += 4828.03
        newid        += 1
        

#BETWEEN/INTERSECT
print "Vertical Lines - Between/Intersect"
for c in xrange(101,150):
    TBL_NETWORK = TBL_TEMP_NETWORK % c
    TBL_PAIRS = TBL_TEMP_PAIRS % c
    
    #find extents of bounding box around island
    Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(TBL_NETWORK)
    cur.execute(Q_ExtentCoords)
    bbox_json = cur.fetchall()
    bbox = json.loads(bbox_json[0][0])
    xmin = min(zip(*bbox['coordinates'][0])[0])
    xmax = max(zip(*bbox['coordinates'][0])[0])
    ymin = min(zip(*bbox['coordinates'][0])[1])
    ymax = max(zip(*bbox['coordinates'][0])[1])

    iterations = int(math.ceil((xmax-xmin)/4828.03))
    #OD LINES THAT INTERSECT BREAK LINES

    #starting y value of line
    #for intersecting OD lines, start at the second line
    x_value = xmin + 4828.03
    #newid starting at 1 is for intersection/overlap sections
    newid = 1
    #loop over break lines selecting OD lines that intersect them
    for z in xrange(1,iterations):
        
        print c, newid
        
        TBL_TEMP_PAIRS_2 = "temp_pairs_502_%d_%d" % (c, newid)
        TBL_TEMP_NETWORK_2 = "temp_network_502_%d_%d" % (c, newid)

        
        cur.execute(Q_IntersectLines.format(TBL_PAIRS) % (x_value, ymin, x_value, ymax))
        intersect_pairs = cur.fetchall()
        
        print "Inserting Pairs"
        print len(intersect_pairs)
        
        if len(intersect_pairs) > 0:
            cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
            connection.commit()
            cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
            connection.commit()
            
            str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
            cur.execute("""BEGIN TRANSACTION;""")
            batch_size = 10000
            for i in xrange(0, len(intersect_pairs), batch_size):
                j = i + batch_size
                arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in intersect_pairs[i:j])
                #print arg_str
                Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
                cur.execute(Q_Insert)
            connection.commit()
            
            print "Creating Extent"
            
            Q_CountPairs = ("""SELECT COUNT(*) FROM "{0}";""").format(TBL_TEMP_PAIRS_2)
            cur.execute(Q_CountPairs)
            NumPairs = cur.fetchall()
            
            #check to see if there are pairs to put a bounding box around
            if int(NumPairs[0][0]) > 0:
                # bounding box around OD lines that intersect the line with 1 mile buffer
                cur.execute(Q_BBoxExtent.format(TBL_TEMP_PAIRS_2))
                intersect_bbox_json = cur.fetchall()
                intersect_bbox = json.loads(intersect_bbox_json[0][0])
                inter_xmin = min(zip(*intersect_bbox['coordinates'][0])[0])
                inter_xmax = max(zip(*intersect_bbox['coordinates'][0])[0])
                inter_ymin = min(zip(*intersect_bbox['coordinates'][0])[1])
                inter_ymax = max(zip(*intersect_bbox['coordinates'][0])[1])
                
                print "Clipping Network"
                
                cur.execute(Q_ClipNetwork.format(TBL_NETWORK) % (
                    (inter_xmin - 1609.34),
                    inter_ymin, 
                    (inter_xmin - 1609.34), 
                    inter_ymax, 
                    (inter_xmax + 1609.34), 
                    inter_ymin,
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
                connection.commit()
                
                print "Updating SRID"
                
                cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
                cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
        
        #if not, drop the tables for that chunk
            else: 
                print "No Pairs in chunk"
            # cur.execute("""DROP TABLE public."{0}";""").format(TBL_TEMP_PAIRS_2)
            # cur.execute("""DROP TABLE public."{0}";""").format(TBL_TEMP_NETWORK_2)

        
        #update values for next iteration
        x_value  += 4828.03
        newid    += 1

#BETWEEN/BETWEEN
print "Vertical Lines - Between/Between"
for c in xrange(101,150):
    TBL_NETWORK = TBL_TEMP_NETWORK % c
    TBL_PAIRS = TBL_TEMP_PAIRS % c

    #find extents of bounding box around island
    Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(TBL_NETWORK)
    cur.execute(Q_ExtentCoords)
    bbox_json = cur.fetchall()
    bbox = json.loads(bbox_json[0][0])
    xmin = min(zip(*bbox['coordinates'][0])[0])
    xmax = max(zip(*bbox['coordinates'][0])[0])
    ymin = min(zip(*bbox['coordinates'][0])[1])
    ymax = max(zip(*bbox['coordinates'][0])[1])

    iterations = int(math.ceil((xmax-xmin)/4828.03))

    #OD LINES IN BETWEEN BREAK LINES

    #starting y value of line
    x_value_left = xmin
    x_value_right = x_value_left + 4828.03
    #newid starting at 100 is for between sections
    newid = 101
    #loop over break lines selecting OD lines that are between them
    for z in xrange(1, iterations+1):

        print c ,newid

        TBL_TEMP_PAIRS_2 = "temp_pairs_502_%d_%d" % (c, newid)
        TBL_TEMP_NETWORK_2 = "temp_network_502_%d_%d" % (c, newid)

        
        cur.execute(Q_LinesBetween.format(TBL_PAIRS) % (
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
        
        if len(between_pairs) > 0:
            cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS_2))
            connection.commit()
            cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK_2))
            connection.commit()

            str_rpl = "(%s, %s, ST_GeomFromGeoJSON('%s'))"
            cur.execute("""BEGIN TRANSACTION;""")
            batch_size = 10000
            for i in xrange(0, len(between_pairs), batch_size):
                j = i + batch_size
                arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in between_pairs[i:j])
                #print arg_str
                Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff, geom) VALUES {1};""".format(TBL_TEMP_PAIRS_2, arg_str)
                cur.execute(Q_Insert)
            connection.commit()
            
            print "Clipping Network"    
            
            #clip network with 1 mile buffer on top and bottom      
            cur.execute(Q_ClipNetwork.format(TBL_NETWORK) % (
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
            connection.commit()
            
            print "Updating SRID"
            
            cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS_2))
            cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK_2))
        
        else:
            print "No pairs in chunk"
        
        x_value_left  += 4828.03
        x_value_right     += 4828.03
        newid        += 1
        



