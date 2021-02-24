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
TBL_EDGE = "edgecounts_bigisland"
TBL_EDGE_IPD = "edges_ipd_bigisland"
TBL_RESULTS = "results_bigisland"
TBL_RESULTS_IPD = "results_ipd_bigisland"

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
#Q_Level3Links = """
#    CREATE TABLE "{0}" AS
#        SELECT *
#        FROM "{1}"
#        WHERE linklts > 0.3 AND linklts <= 0.6;
#    COMMIT;
#""".format(TBL_LTS3, TBL_COUNTLTS)
#cur.execute(Q_Level3Links)

#################################################
#combine small and bigisland results into a single results table (repeat for IPD)
'''CREATE TABLE results_all AS(
SELECT *
FROM results
UNION ALL
SELECT *
FROM results_bigisland);
COMMIT;'''

'''CREATE TABLE results_ipd_all AS(
SELECT *
FROM results_ipd
UNION ALL
SELECT *
FROM results_ipd_bigisland);
COMMIT;'''
#################################################


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#select top 10 percent from county lts result tables to identify priority links to work with
def priorities(analysis, string):
    print string
    
    countsbycounty = []
    regionaltop50percent = []

    RESULT_TBL = analysis

    counties = ('Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia', 'Burlington', 'Camden', 'Gloucester', 'Mercer')
    #count how many are in the top 50 percent of LTS 3 roads in each county
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
        """.format(counties[i], RESULT_TBL, (countsbycounty[i][0][0]*5))
        cur.execute(Q_CompileTop10Percent)
        output = cur.fetchall()
        
        print "Sorting"
        stop = countsbycounty[i][0][0]
        counter = 0
        for item in output:
            row = []
            for thing in item:
                row.append(thing)
            counter += 1
            if counter <= stop:
                row.append(10)
            elif counter > stop  and counter <= (stop*2):
                row.append(20)
            elif counter > (stop*2) and counter <= (stop*3):
                row.append(30)
            elif counter > (stop*3) and counter <= (stop*4):
                row.append(40)
            elif counter > (stop*4):
                row.append(50)
            regionaltop50percent.append(row)
    
    print "Writing"    
    PRIORITY_RESULTS = 'priorities_'+string
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
      geom geometry(Geometry,26918),
      priority integer
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    COMMIT;
    """.format(PRIORITY_RESULTS)
    cur.execute(Q_CreateOutputTable)

    str_rpl = "(%s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT('%s'), %s)"
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 100
    for i in xrange(0, len(regionaltop50percent), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in regionaltop50percent[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO public."{0}" (edge, total, linklts, length, totnumlane, bikefac, speed_lts, geom, priority) VALUES {1}""".format(PRIORITY_RESULTS, arg_str)
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
priorities('results_all', 'all')
priorities('results_ipd_all', 'all_ipd')


###################combine transit priorities results into a single table###############################
Q_addcol = """
ALTER TABLE %s
ADD COLUMN transitmode character varying;
COMMIT;"""

Q_updatecol = """
UPDATE %s
SET transitmode = '%s';
COMMIT;
"""

TBL = 'priorities_rail'
TBL = 'priorities_rail_ipd'
string = "rail"
TBL = 'priorities_transit'
TBL = 'priorities_transit_ipd'
string = "bus"
TBL = 'priorities_trolley'
TBL = 'priorities_trolley_ipd'
string = "trolley"
cur.execute(Q_addcol % (TBL))
cur.execute(Q_updatecol % (TBL, string))


Q_CombineTransit = """
CREATE TABLE priotities_alltransit AS(
    WITH rail_bus AS(
        SELECT 
            r.edge,
            (r.total + t.total) AS total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            CONCAT(r.transitmode, ', ', t.transitmode) AS mode,
            r.geom
        FROM priorities_rail r
        INNER JOIN priorities_transit t
        ON r.edge = t.edge),
    rail_trolley AS(
        SELECT 
            r.edge,
            (r.total + t.total) AS total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            CONCAT(r.transitmode, ', ', t.transitmode) AS mode,
            r.geom
        FROM priorities_rail r
        INNER JOIN priorities_trolley t
        ON r.edge = t.edge),
    trolley_bus AS(
        SELECT 
            r.edge,
            (r.total + t.total) AS total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            CONCAT(r.transitmode, ', ', t.transitmode) AS mode,
            r.geom
        FROM priorities_tolley r
        INNER JOIN priorities_transit t
        ON r.edge = t.edge),
    rail_only AS(
        SELECT r.edge, 
            r.total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            r.transitmode,
            r.geom
        FROM priorities_rail r
        WHERE r.edge NOT IN(
            SELECT t.edge
            FROM priorities_trolley t)
        AND r.edge NOT IN(
            SELECT edge
            FROM priorities_transit)
        ),
    trolley_only AS(
        SELECT r.edge, 
            r.total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            r.transitmode,
            r.geom
        FROM priorities_trolley r
        WHERE r.edge NOT IN(
            SELECT t.edge
            FROM priorities_rail t)
        AND r.edge NOT IN(
            SELECT edge
            FROM priorities_transit)
        ),
    bus_only AS(
        SELECT r.edge, 
            r.total, 
            r.linklts,
            r.length,
            r.totnumlane,
            r.bikefac,
            r.speed_lts,
            r.transitmode,
            r.geom
        FROM priorities_transit r
        WHERE r.edge NOT IN(
            SELECT t.edge
            FROM priorities_rail t)
        AND r.edge NOT IN(
            SELECT edge
            FROM priorities_trolley)
        ),  

    SELECT * FROM rail_bus
    UNION ALL
    SELECT * FROM rail_trolley
    UNION ALL
    SELECT * FROM trolley_bus
    UNION ALL
    SELECT * FROM rail_only
    UNION ALL
    SELECT * FROM trolley_only
    UNION ALL
    SELECT * FROM bus_only
    );
COMMIT;
"""
#regular results
cur.execute(Q_CombineTransit)
#repeat for ipd results....