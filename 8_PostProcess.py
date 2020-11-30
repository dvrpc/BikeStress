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
string = "school"

TBL_EDGE = "edgecounts_%s" %string
TBL_EDGE_IPD = "edges_ipd_%s" %string
TBL_RESULTS = "results_%s" %string
TBL_RESULTS_IPD = "results_ipd_%s" %string
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
counties = ('Bucks', 'Chester', 'Delaware', 'Montgomery', 'Philadelphia', 'Burlington', 'Camden', 'Gloucester', 'Mercer')
resulttables = ('results_rail', 'results_trolley')
countsbycounty = []

for i in xrange(len(counties)):
    for j in xrange(len(resulttables)):
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
        FROM tblC""".format(counties[i], resulttables[j])
        cur.execute(Q_CountTop10Percent)
        count = cur.fetchall()
        countsbycounty.append(count)

counter = 0
regionaltop10percent = []
for i in xrange(len(counties)):
    for j in xrange(len(resulttables)):
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
        WHERE linklts > 0.3 AND linklts <= 0.6)
        ORDER BY total DESC LIMIT {2};
        """.format(counties[i], resulttables[j], countsbycounty[counter])
        cur.execute(Q_CompileTop10Percent)
        output = cur.fetchall()
        regionaltop10percent.append(output)
        counter += 1


#which lts3 segments are in the top10% in their respective County
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


###which L1&2 islands would be connected by LTS 3 priority segments (top 10% only)

##SELECT ALL THE LINKS THAT ARE PART OF EACH ISLAND
TBL_CON_ISLANDS = "con_islands_trails"
TBL_LTS3 = "lts3_trails_results"
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

cur.execute(Q_BUFFER_INTERSECT.format(TBL_LTS3, TBL_MASTERLINKS_GROUPS, TBL_CON_ISLANDS))

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
