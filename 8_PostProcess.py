#####RUN PIECE BY PIECE#####

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
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#string for special destination table names
#transit, rail, trolley, trail, school, main
# string = "school"

# TBL_EDGE = "edgecounts_%s" %string
# TBL_EDGE_IPD = "edges_ipd_%s" %string
# TBL_RESULTS = "results_%s" %string
# TBL_RESULTS_IPD = "results_ipd_%s" %string
TBL_EDGE = "edgecounts"
TBL_EDGE_IPD = "edges_ipd"
TBL_RESULTS = "results"
TBL_RESULTS_IPD = "results_ipd"

TBL_MASTERLINKS_GROUPS = "master_links_grp"
TBL_ALL_LINKS = "links"

#summarize results and join to geometries and link attributes
Q_Summarize = """
    CREATE TABLE "{0}" AS (
        WITH tblA AS(
            SELECT 
                edge, 
                SUM(count) AS total
            FROM public."{1}"
            GROUP BY edge
            ),
        tblB AS(
        SELECT
            a.*,
            m.cost,
            m.geom
            FROM tblA a
            INNER JOIN "{2}" m
            ON m.mixid = a.edge)
        SELECT use.edge, use.total, links.linklts, links.length, links.totnumlane, links.bikefac, links.speed_lts, use.geom
        FROM tblB use
        INNER JOIN "{3}" links
        ON use.edge = links.gid)
        ;
""".format(TBL_RESULTS, TBL_EDGE, TBL_MASTERLINKS_GROUPS, TBL_ALL_LINKS)

#same for IPD results
Q_Summarize_IPD = """
    CREATE TABLE "{0}" AS (
        WITH tblA AS(
            SELECT 
                edge, 
                SUM(ipdweight) AS total
            FROM public."{1}"
            GROUP BY edge
            ),
        tblB AS(
        SELECT
            a.*,
            m.cost,
            m.geom
            FROM tblA a
            INNER JOIN "{2}" m
            ON m.mixid = a.edge)
        SELECT use.edge, use.total, links.linklts, links.length, links.totnumlane, links.bikefac, links.speed_lts, use.geom
        FROM tblB use
        INNER JOIN "{3}" links
        ON use.edge = links.gid)
        ;
""".format(TBL_RESULTS_IPD, TBL_EDGE_IPD, TBL_MASTERLINKS_GROUPS, TBL_ALL_LINKS)

cur.execute(Q_Summarize)
con.commit()
cur.execute(Q_Summarize_IPD)
con.commit()

#pull just LTS 3 links for visualization later
Q_Level3Links = """
    CREATE TABLE "{0}" AS
        SELECT *
        FROM "{1}"
        WHERE linklts > 0.3 AND linklts <= 0.6;
    COMMIT;
""".format(TBL_LTS3, TBL_COUNTLTS)
cur.execute(Q_Level3Links)


#################################################
TBL_USE = "linkuse"
TBL_USE_IPD = "linkuse_ipd"
TBL_COUNTLTS = "linkuse_lts"
TBL_LTS3 = "LTS3only_linkuse"
TBL_CON_ISLANDS = "connected_islands"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#select top 10 percent from county lts result tables to identify priority links to work with
def priorities(analysis, string):
    print string
    
    countsbycounty = []
    regionaltop10percent = []

    RESULT_TBL = analysis

    counties = ('Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia', 'Burlington', 'Camden', 'Gloucester', 'Mercer')
    #count how many are in the top 10 percent of LTS 3 roads in each county
    for i in xrange(len(counties)):
        print "Counting ", counties[i]
        Q_CountTop10Percent = """
        WITH county AS (
            SELECT *
            FROM county_boundaries
            WHERE co_name = '{0}'),
        tblB AS (
            SELECT r.*
            FROM {1} AS r
            JOIN county as c
            ON st_within (r.geom, c.geom)),
        tblC AS( 
            SELECT *
            FROM tblB
            WHERE linklts > 0.3 AND linklts <= 0.6)
        SELECT COUNT(*)/10 AS top10percent
        FROM tblC""".format(counties[i], RESULT_TBL)
        cur.execute(Q_CountTop10Percent)
        count = cur.fetchall()
        countsbycounty.append(count)
    #select that many from each county
    for i in xrange(len(counties)):
        print "Compiling ", counties[i], countsbycounty[i][0][0]
        Q_CompileTop10Percent = """
        WITH county AS (
            SELECT *
            FROM county_boundaries
            WHERE co_name = '{0}'),
        tblB AS (
            SELECT r.*
            FROM {1} AS r
            JOIN county as c
            ON st_within (r.geom, c.geom))
        SELECT *
        FROM tblB
        WHERE linklts > 0.3 AND linklts <= 0.6
        ORDER BY total DESC LIMIT {2};
        """.format(counties[i], RESULT_TBL, countsbycounty[i][0][0])
        cur.execute(Q_CompileTop10Percent)
        output = cur.fetchall()
        for item in output:
            regionaltop10percent.append(item)
    #change for mail run to elminate use of "string"    
    #PRIORITY_RESULTS = 'priorities_'+string
    PRIORITY_RESULTS = 'priotities_main'
    #write priorities into a table
    Q_CreateOutputTable = """
    CREATE TABLE IF NOT EXISTS public."{0}"
    (
      edge integer,
      total bigint,
      linklts double precision,
      length double precision,
      totnumlane smallint,
      bikefac smallint,
      speed_lts smallint,
      geom geometry(Geometry,26918)
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    COMMIT;
    """.format(PRIORITY_RESULTS)
    cur.execute(Q_CreateOutputTable)

    str_rpl = "(%s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT('%s'))"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 100
    for i in xrange(0, len(regionaltop10percent), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in regionaltop10percent[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO public."{0}" (edge, total, linklts, length, totnumlane, bikefac, speed_lts, geom) VALUES {1}""".format(PRIORITY_RESULTS, arg_str)
        cur.execute(Q_Insert)
    cur.execute("COMMIT;")
    
#running function
priorities('results_rail', 'rail')
priorities('results_school', 'school')
priorities('results_trail', 'trail')
priorities('results_transit', 'transit')
priorities('results_trolley', 'trolley')
priorities('results_ipd_rail', 'rail_ipd')
priorities('results_ipd_school', 'school_ipd')
priorities('results_ipd_trail', 'trail_ipd')
priorities('results_ipd_transit', 'transit_ipd')
priorities('results_ipd_trolley', 'trolley_ipd')

#also run for main run




###which L1&2 islands would be connected by LTS 3 priority segments (top 10% only)
#takes a long time to run (~14 minutes on just rail priorities) - maybe save for later; not 100% sure if this is worth it and how much it's used. 
#might be better to just look at it visually.
##SELECT ALL THE LINKS THAT ARE PART OF EACH ISLAND
string = rail
TBL_CON_ISLANDS = 'con_islands_'+string
PRIORITY_RESULTS = 'priorities_'+string
TBL_MASTERLINKS_GROUPS = "l2_master_links_grp"

Q_BUFFER_INTERSECT = """
    CREATE TABLE {2} AS(
        WITH buf AS(
            SELECT edge, st_buffer(geom, 10) buffer
            FROM "{0}"),

        tblA AS(
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
                FROM {1} L
            INNER JOIN (
                SELECT 
                edge,
                buffer
                FROM buf) B
            ON ST_Intersects(L.geom, B.buffer)) goo)

        SELECT 
            tblA.edge,
            string_agg(strong::text, ', ') AS islands,
            g.geom
        FROM tblA
        INNER JOIN "{0}" g
        ON tblA.edge = g.link
        GROUP BY tblA.edge, g.geom
);
"""

cur.execute(Q_BUFFER_INTERSECT.format(PRIORITY_RESULTS, TBL_MASTERLINKS_GROUPS, TBL_CON_ISLANDS))

#do we just want to do this for the 'priorities'? it might be better to have for everything, and just leave it blank where it does not apply
Q_JOIN = """
CREATE TABLE {0}
    SELECT l.*, c.islands
    FROM "{1}" l
    FULL JOIN {2} c
    ON l.edge = c.edge
    );
"""
cur.execute(Q_JOIN.format(TBL_RESULTS, TBL_LTS3, TBL_CON_ISLANDS))
