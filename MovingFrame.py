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
logger = multiprocessing.log_to_stderr(logging.INFO)

TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco_L3_tolerablelinks"
TBL_SPATHS = "montco_L3_shortestpaths"
TBL_TOLNODES = "montco_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_geoffs"
TBL_GEOFF_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_GROUPS = "montco_groups"
TBL_GEOFF_PAIRS = "montco_L3_geoff_pairs"
TBL_OD_LINES = "montco_L3_grp_OD_lines"
TBL_NODENOS = "montco_L3_nodenos"
TBL_OD = "montco_L3_OandD"
TBL_GEOFF_NODES = "montco_L3_geoff_nodes"
TBL_NODES_GID = "montco_L3_nodes_gid"
TBL_NODES_GEOFF = "montco_L3_nodes_geoff"


#VIEW = "links_l3_grp_%s" % str(sys.argv[1])
VIEW = "links_l3_grp_196"
#????
# TBL_TEMP_PAIRS = "temp_pairs_%s_%s" % (str(sys.argv[1]), chunk_id)
# TBL_TEMP_NETWORK = = "temp_network_%s_%s" % (str(sys.argv[1]), chunk_id)
TBL_TEMP_PAIRS = "temp_pairs_196_%s" % chunk_id
TBL_TEMP_NETWORK = = "temp_network_196_%s" % chunk_id

con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODENOS)
cur.execute(Q_GetList)
nodenos = cur.fetchall()

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODES_GEOFF)
cur.execute(Q_GetList)
nodes_geoff_list = cur.fetchall()
nodes_geoff = dict(nodes_geoff_list)

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODES_GID)
cur.execute(Q_GetList)
nodes_gids_list = cur.fetchall()
nodes_gids = dict(nodes_gids_list)

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_GEOFF_NODES)
cur.execute(Q_GetList)
geoff_nodes_list = cur.fetchall()
geoff_nodes = dict(geoff_nodes_list)

#call OD list from postgres
Q_GetOD = """
    SELECT * FROM "{0}";
    """.format(TBL_OD)
cur.execute(Q_GetOD)
OandD = cur.fetchall()

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
geoff_grp = dict(cur.fetchall())

CloseEnough = []
DiffGroup = 0
NullGroup = 0
#are the OD geoffs in the same group? if so, add pair to list to be calculated
for i, (fromnodeindex, tonodeindex) in enumerate(OandD):
    #if i % pool_size == (worker_number - 1):
    fromnodeno = nodenos[fromnodeindex][0]
    tonodeno = nodenos[tonodeindex][0]
    if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
        if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
            # if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
            if geoff_grp[nodes_geoff[fromnodeno]] == 196:
                CloseEnough.append([
                    nodes_gids[fromnodeno],    # FromGID
                    #fromnodeno,                # FromNode
                    nodes_geoff[fromnodeno],  # FromGeoff
                    nodes_gids[tonodeno],      # ToGID
                    #tonodeno,                  # ToNode
                    nodes_geoff[tonodeno],    # ToGeoff
                    geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                    ])
        else:
            DiffGroup += 1
    else:
        NullGroup += 1
        
del nodenos, OandD, geoff_grp, nodes_geoff

pairs = []
for i, (fgid, fgeoff, tgid, tgeoff, grp) in enumerate(CloseEnough):
    source = fgeoff
    target = tgeoff
    pairs.append((source, target))


#create table in DB to hold pairs
Q_Pairs = """
    CREATE TABLE public."{0}"
    (
      id BIGSERIAL PRIMARY KEY,
      fromgeoff integer,
      togeoff integer
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public."{0}"
      OWNER TO postgres;""".format(TBL_GEOFF_PAIRS)
cur.execute(Q_Pairs)
con.commit()

Q_Insert = """INSERT INTO "{0}" (fromgeoff, togeoff) VALUES (%s, %s);""".format(TBL_GEOFF_PAIRS)

for fg, tg in pairs:
    cur.execute(Q_Insert % (fg, tg))
    
con.commit()

#create table in DB to hold OD lines
Q_ODLines = """
    CREATE TABLE "{0}" AS(
        WITH unique_geoms AS (
            SELECT
                geoffid,
                geom
            FROM "montco_L3_geoffs_viageom"
            GROUP BY geoffid, geom
        )
        SELECT *
        FROM(
            SELECT
                p.fromgeoff,
                p.togeoff,
                ST_MakeLine(g1.geom, g2.geom) AS geom
            FROM "{1}" as p
            INNER JOIN unique_geoms AS g1
            ON p.fromgeoff = g1.geoffid
            INNER JOIN unique_geoms AS g2
            ON p.togeoff = g2.geoffid
        ) AS pair_f);
        """.format(TBL_OD_LINES, TBL_GEOFF_PAIRS)
cur.execute(Q_ODLines)
#add primary key somehow???
con.commit()


#find extents of bounding box around island
Q_ExtentCoords = """SELECT st_asgeojson(st_setsrid(st_extent(geom), 26918)) FROM public."{0}";""".format(VIEW)
cur.execute(Q_ExtentCoords)
bbox_json = cur.fetchall()
bbox = json.loads(bbox_json[0][0])
xmin = min(zip(*bbox['coordinates'][0])[0])
xmax = max(zip(*bbox['coordinates'][0])[0])
ymin = min(zip(*bbox['coordinates'][0])[1])
ymax = max(zip(*bbox['coordinates'][0])[1])



#select OD lines that intersect the break line
Q_IntersectLines = """
    SELECT
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom && ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
    """.format(TBL_OD_LINES)
    
Q_LinesBetween = """
    SELECT 
        fromgeoff,
        togeoff,
        ST_AsGeoJSON(geom)
    FROM public."{0}"
    WHERE geom |>> ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918)
        AND geom <<| ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
    """.format(VIEW)
    
# Q_LinesAbove = 

# Q_LinesBelow = 

#use 5 mile when clipping network to capture full paths
Q_ClipNetwork = """
    SELECT 
        mixid,
        fromgeoff,
        togeoff,
        cost,
        ST_AsGeoJSON(geom),
        strong
    FROM public."{0}"
    WHERE geom |>> ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918)
        AND geom <<| ST_SetSRID(ST_MakeLine(ST_Point(%d, %d),ST_Point(%d, %d)), 26918);
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

Q_SelectTempPairs = """SELECT fromgeoff, togeoff, geom FROM public."{0}";"""

Q_CreateTempNetwork = """
    CREATE TABLE public."{0}"
    (
      id BIGSERIAL PRIMARY KEY, 
      mixid integer,
      fromgeoff integer,
      togeoff integer,
      cost float,
      geom geometry
    )
    WITH (
      OIDS=FALSE
    );
    ALTER TABLE public."{0}"
      OWNER TO postgres;""" 
      
Q_InsertTempNetwork = """INSERT INTO "{0}" (mixid, fromgeoff, togeoff, cost, geom) VALUES (%s, %s, %s, %s, ST_GeomFromGeoJSON('%s'));"""

# Q_DropTempODLines = 

# Q_DropTempNetwork = 


iterations = int(math.ceil((ymax-ymin)/8046.72))

#OD LINES THAT INTERSECT BREAK LINES

#starting y value of line
#for intersecting OD lines, start at the second line
y_value = ymin + 8046.72
#chunk_id starting at 1 is for intersection/overlap sections
chunk_id = 1
#loop over break lines selecting OD lines that intersect them
for i in xrange(1,iterations):
    
    TBL_TEMP_PAIRS = "temp_pairs_196_%d" % chunk_id
    TBL_TEMP_NETWORK = "temp_network_196_%d" % chunk_id
    cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS))
    con.commit()
    cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK))
    con.commit()
    
    cur.execute(Q_IntersectLines % (xmin, y_value, xmax, y_value))
    intersect_pairs = cur.fetchall()
    for fg, tg, geom in intersect_pairs:
        cur.execute(Q_InsertTempPairs.format(TBL_TEMP_PAIRS) % (fg, tg, geom))
    con.commit()
    
    # bounding box around OD lines that intersect the line with 1 mile buffer
    cur.execute(Q_BBoxExtent.format(TBL_TEMP_PAIRS))
    intersect_bbox_json = cur.fetchall()
    intersect_bbox = json.loads(intersect_bbox_json[0][0])
    inter_xmin = min(zip(*intersect_bbox['coordinates'][0])[0])
    inter_xmax = max(zip(*intersect_bbox['coordinates'][0])[0])
    inter_ymin = min(zip(*intersect_bbox['coordinates'][0])[1])
    inter_ymax = max(zip(*intersect_bbox['coordinates'][0])[1])
    
    cur.execute(Q_ClipNetwork % (
        inter_xmin, 
        (inter_ymin - 250), 
        inter_xmax, 
        (inter_ymin - 250), 
        inter_xmin, 
        (inter_ymax + 250), 
        inter_xmax, 
        (inter_ymax + 250)
    ))
    
    clip_network = cur.fetchall()
    for mixid, fg, tg, cost, geom, strong in clip_network:
        cur.execute(Q_InsertTempNetwork.format(TBL_TEMP_NETWORK) % (mixid, fg, tg, cost, geom))
    con.commit()
    
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS))
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK))
    
    #update values for next iteration
    y_value        += 8046.72
    chunk_id       += 1


#OD LINES IN BETWEEN BREAK LINES

#starting y value of line
y_value_bottom = ymin
y_value_top = y_value_bottom + 8046.72
#chunk_id starting at 100 is for between sections
chunk_id = 100
#loop over break lines selecting OD lines that are between them
for i in xrange(1, iterations+1):

    TBL_TEMP_PAIRS = "temp_pairs_196_%d" % chunk_id
    TBL_TEMP_NETWORK = "temp_network_196_%d" % chunk_id
    cur.execute(Q_CreateTempODLinesTable.format(TBL_TEMP_PAIRS))
    con.commit()
    cur.execute(Q_CreateTempNetwork.format(TBL_TEMP_NETWORK))
    con.commit()
    
    cur.execute(Q_LinesBetween % (
        xmin,
        y_value_bottom,
        xmax,
        y_value_bottom,
        xmin,
        y_value_top,
        xmax,
        y_value_top
    ))
    
    between_pairs = cur.fetchall()
    for fg, tg, geom in between_pairs:
        cur.execute(Q_InsertTempPairs.format(TBL_TEMP_PAIRS) % (fg, tg, geom))
    
    #clip network with 250m buffer on top and bottom      
    cur.execute(Q_ClipNetwork % (
        xmin, 
        (y_value_bottom - 250),
        xmax,
        (y_value_bottom - 250),
        xmin,
        (y_value_top + 250),
        xmax,
        (y_value_top + 250)
    ))
    
    clip_network = cur.fetchall()
    for mixid, fg, tg, cost, geom, strong in clip_network:
        cur.execute(Q_InsertTempNetwork.format(TBL_TEMP_NETWORK) % (mixid, fg, tg, cost, geom))
    
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_PAIRS))
    cur.execute("""SELECT UpdateGeometrySRID('{0}', 'geom', 26918); """.format(TBL_TEMP_NETWORK))
    
    y_value_bottom  += 8046.72
    y_value_top     += 8046.72
    chunk_id        += 1
    
    
#run network x child over these temp tables
#then drop the temp tables?

