import multiprocessing
import multiprocessing.dummy
# import threading
import Queue
import psycopg2 as psql
import time
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
TBL_MASTERLINKS_GROUPS = "montco_master_links_grp"
TBL_GROUPS = "montco_groups"




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
            if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
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

"""
CREATE TABLE public."montco_L3_geoff_pairs"
(
  fromgeoff integer,
  togeoff integer
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public."montco_L3_geoff_pairs"
  OWNER TO postgres;"""

con.commit()

TBL_GEOFF_PAIRS = "montco_L3_geoff_pairs"
  
Q_Insert = """INSERT INTO "{0}" VALUES (%s, %s);""".format(TBL_GEOFF_PAIRS)

for fg, tg in pairs:
    cur.execute(Q_Insert % (fg, tg))
    
con.commit()


# join with geometries from geoff_via_geom table and create lines between pairs
# in SQL
CREATE TABLE "montco_L3_OD_lines" AS(
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
        FROM "montco_L3_geoff_pairs" as p
        INNER JOIN unique_geoms AS g1
        ON p.fromgeoff = g1.geoffid
        INNER JOIN unique_geoms AS g2
        ON p.togeoff = g2.geoffid
    ) AS pair_f);
    
COMMIT;


#create bounding box around all OD lines in QGIS
SELECT st_setsrid(st_extent(geom) , 26918) AS lines_extent FROM public."montco_L3_OD_lines";

#use bottom corner location and width to start creating grid
SELECT ST_XMax(st_setsrid(st_extent(geom) , 26918)) AS xmax FROM public."montco_L3_OD_lines"
xmax
455720.25
SELECT ST_YMax(st_setsrid(st_extent(geom) , 26918)) AS ymax FROM public."montco_L3_OD_lines"
ymax
4475690.6012
SELECT ST_XMin(st_setsrid(st_extent(geom) , 26918)) AS xmin FROM public."montco_L3_OD_lines"
xmin
439910.789725
SELECT ST_YMin(st_setsrid(st_extent(geom) , 26918)) AS ymin FROM public."montco_L3_OD_lines"
ymin
4452772.41455


#lower left = xmin, ymin
439910.789725, 4452772.41455

#xmax - xmin
15809.460275

#ymin + 5 miles (8046.72 meters)
4460819.13455

#upper right = xmax, ymin+5
455720.25, 4460819.13455

#ymin + 10 miles
4468865.854549999

#create a single small box using lower left and upper rght from above
SELECT ST_SetSRID(ST_MakeBox2D(ST_Point(439910.789725, 4452772.41455),ST_Point(455720.25, 4460819.13455)), 26918) ;

#select lines that intersect this small box without drawing it
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom && ST_SetSRID(ST_MakeBox2D(ST_Point(439910.789725, 4452772.41455),ST_Point(455720.25, 4460819.13455)), 26918) 

#select lines that are just within this box (overlaps and below the upper right point)
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom &<| ST_SetSRID(ST_MakeBox2D(ST_Point(439910.789725, 4452772.41455),ST_Point(455720.25, 4460819.13455)), 26918) 

#line's bounding box is strictly above the box
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom |>> ST_SetSRID(ST_MakeBox2D(ST_Point(439910.789725, 4452772.41455),ST_Point(455720.25, 4460819.13455)), 26918) 

#operators might be more straight forward if you sum in the ST_MakeBox2D with ST_MakeLine
 

#try new operators to narrow down
#or use st_makeline and select lines that are below, above, or cross?

#for ST_MakeLine, use (xmin, ymin + 5) and (xmax, ymin + 5)
SELECT ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4460819.13455),ST_Point(455720.25, 4460819.13455)), 26918) ;


#using line, select OD lines that intersect the line
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom && ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4460819.13455),ST_Point(455720.25, 4460819.13455)), 26918) ;

#using line, select OD lines that are below the line
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom <<| ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4460819.13455),ST_Point(455720.25, 4460819.13455)), 26918) ;

#using line, select OD lines that are above one line and below the other
SELECT *
FROM public."montco_L3_OD_lines"
WHERE geom |>> ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4460819.13455),ST_Point(455720.25, 4460819.13455)), 26918)
    AND geom <<| ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4468865.854549999),ST_Point(455720.25, 4468865.854549999)), 26918);

#make next line
#for ST_MakeLine, use (xmin, ymin + 10) and (xmax, ymin + 10)
SELECT ST_SetSRID(ST_MakeLine(ST_Point(439910.789725, 4468865.854549999),ST_Point(455720.25, 4468865.854549999)), 26918) ;



#repeat until lower left xmin > xmax









