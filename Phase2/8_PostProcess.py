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

####table names to modify in subsequent runs###
TBL_EDGE = "edgecounts_testarea"
TBL_EDGETOTAL = "edgetotals_testarea"
TBL_USE = "linkuse_testarea"
TBL_COUNTLTS = "linkuse_lts_testarea"
TBL_LTS3 = "LTS3only_linkuse_testarea"
TBL_CON_ISLANDS = "connected_islands_testarea"

#most of these are geared towards L2 for island visualization unless otherwise noted
TBL_ALL_LINKS = "testarea_links"
TBL_LINKS = "l2_tolerablelinks_testarea"
TBL_NODES = "testarea_nodes"
TBL_TOLNODES = "l2_tol_nodes_testarea"
TBL_GEOFF_LOOKUP = "l2_geoffs_testarea"
TBL_GEOFF_LOOKUP_GEOM = "l2_geoffs_viageom_testarea"
TBL_MASTERLINKS = "l2_master_links_testarea"
TBL_MASTERLINKS_GEO = "l2_master_links_geo_testarea"
TBL_MASTERLINKS_GROUPS = "l2_master_links_grp_testarea"
TBL_MASTERLINKS_GROUPS_L3 = "master_links_grp_testarea"
TBL_GROUPS = "l2_groups_testarea"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "l2_tolerableturns_testarea"

#index names
IDX_LINKS_geom = "l2tol_links_geom_idx_ta"
IDX_LINKS_value = "l2tol_links_value_idx_ta"
IDX_TOL_NODES_geom = "l2tolnodes_geom_idx_ta"
IDX_TOL_NODES_value = "l2tolnodes_value_idx_ta"


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#sum the counts of unique edges in the edge count table that was built upon by each run of 7_CountEdges.py on the subsets of the shortest path results
Q_UniqueEdgeSum = """
    CREATE TABLE "{0}" AS
        SELECT edge, SUM(count) AS total
        FROM "{1}"
        GROUP BY edge;
    COMMIT;
""".format(TBL_EDGETOTAL, TBL_EDGE)
cur.execute(Q_UniqueEdgeSum)

#join back to link table to get geometry for display purposes and linklts for filtering purposes
Q_GeomJoin = """
    CREATE TABLE "{0}" AS
        SELECT edges.*, "{1}".cost, "{1}".geom 
        FROM (
        SELECT * FROM "{2}"
        ) AS edges 
        INNER JOIN "{1}"
        ON "{1}".mixid = edges.edge;
    COMMIT;
""".format(TBL_USE, TBL_MASTERLINKS_GROUPS_L3, TBL_EDGETOTAL)
cur.execute(Q_GeomJoin)

#join results to LTS assigned links
Q_JoinLTS = """
    CREATE TABLE "{0}" AS
        SELECT use.edge, use.total, links.linklts, links.length, links.numlanes, links.bike_fac_1, links.speedtouse, use.geom
        FROM "{1}" AS use
        INNER JOIN "{2}" as links
        ON use.edge = links.gid;
    COMMIT;
""".format(TBL_COUNTLTS, TBL_USE, TBL_ALL_LINKS)
cur.execute(Q_JoinLTS)

#pull just LTS 3 links for visualization later
Q_Level3Links = """
    CREATE TABLE "{0}" AS
        SELECT *
        FROM "{1}"
        WHERE linklts > 0.3 AND linklts <= 0.6;
    COMMIT;
""".format(TBL_LTS3, TBL_COUNTLTS)
cur.execute(Q_Level3Links)


#####################COPIED FROM 3_DataSetup.py and altered to create L1&2 master links#####################
#create subset of links based on assigned LTS
##LTS 1 and 2 only

Q_LinkSubset = """
    CREATE TABLE "{0}" AS
        SELECT * FROM "{1}" WHERE linklts <= 0.3 AND linklts >= 0;
    COMMIT;
    CREATE INDEX IF NOT EXISTS "{2}"
        ON public."{0}" USING gist
        (geom)
        TABLESPACE pg_default; 
    CREATE INDEX IF NOT EXISTS "{3}"
        ON public."{0}" USING btree
        (gid, no, fromnodeno, tonodeno, length, linklts)
        TABLESPACE pg_default;    
""".format(TBL_LINKS, TBL_ALL_LINKS, IDX_LINKS_geom, IDX_LINKS_value)
cur.execute(Q_LinkSubset)

#query to select nodes that correspond with the tolerable link subset
Q_NodeSubset = """
    CREATE TABLE "{2}" AS
        SELECT DISTINCT N.* FROM "{0}" N
        INNER JOIN "{1}" TL
        ON N.no = TL.fromnodeno
        
        UNION
        
        SELECT DISTINCT N.* FROM "{0}" N
        INNER JOIN "{1}" TL
        ON N.no = TL.tonodeno;
    COMMIT;
    CREATE INDEX IF NOT EXISTS "{3}"
        ON public."{2}" USING gist
        (geom)
        TABLESPACE pg_default; 
    CREATE INDEX IF NOT EXISTS "{4}"
        ON public."{2}" USING btree
        (gid, no)
        TABLESPACE pg_default; 
""".format(TBL_NODES, TBL_LINKS, TBL_TOLNODES, IDX_TOL_NODES_geom, IDX_TOL_NODES_value)
cur.execute(Q_NodeSubset)

#query to select turns that share a node with the links in the subset of links being used
Q_SubsetTurns = """
CREATE TABLE "{2}" AS
    SELECT * FROM(
        SELECT tblB.fromnode, tblB.vianode, tblB.tonode, tblB.maxapproach, tblB.turndirection, tblB.turnlts FROM (
            SELECT * FROM(
                SELECT DISTINCT(fromnodeno) FROM "{1}"
                UNION
                SELECT DISTINCT(tonodeno) FROM "{1}") AS tblA
            INNER JOIN public."{0}"
            ON public."{0}".fromnode = tblA.fromnodeno) AS tblB
        UNION
        SELECT tblC.fromnode, tblC.vianode, tblC.tonode, tblC.maxapproach, tblC.turndirection, tblC.turnlts FROM(
            SELECT * FROM(
                SELECT DISTINCT(fromnodeno) FROM "{1}"
                UNION
                SELECT DISTINCT(tonodeno) FROM "{1}") AS tblA
            INNER JOIN public."{0}"
            ON public."{0}".vianode = tblA.fromnodeno) AS tblC
        UNION
        SELECT tblD.fromnode, tblD.vianode, tblD.tonode, tblD.maxapproach, tblD.turndirection, tblD.turnlts FROM(
            SELECT * FROM(
                SELECT DISTINCT(fromnodeno) FROM "{1}"
                UNION
                SELECT DISTINCT(tonodeno) FROM "{1}") AS tblA
            INNER JOIN public."{0}"
            ON public."{0}".tonode = tblA.fromnodeno) AS tblD
        ) AS t0;
""".format(TBL_TURNS, TBL_LINKS, TBL_SUBTURNS)
cur.execute(Q_SubsetTurns)
con.commit()

#create unique ID field for turns
Q_AddID = """
    ALTER TABLE "{0}" ADD COLUMN dummy SERIAL;
    ALTER TABLE "{0}" ADD COLUMN turnID integer;
    UPDATE "{0}"
    SET turnID = dummy*-1;
    COMMIT;
""".format(TBL_SUBTURNS)
cur.execute(Q_AddID)
con.commit()

#calcualte turn cost and add to new row in table
#1 = right, 2 = straight, 3 = left
#use cost constant from 0.005 for link length in miles
Q_TurnCost = """    
    ALTER TABLE "{0}" ADD COLUMN cost numeric;
    COMMIT;

    UPDATE "{0}"
    SET cost = (0.005*(1 + "turnlts"))
    WHERE turndirection = 2;
    COMMIT;

    UPDATE "{0}"
    SET cost = (0.005*(1 + 1 + "turnlts"))
    WHERE turndirection = 1;
    COMMIT;

    UPDATE "{0}"
    SET cost = (0.005*(1 + 2 + "turnlts"))
    WHERE turndirection = 3;
    COMMIT;
""".format(TBL_SUBTURNS)
cur.execute(Q_TurnCost)

#query to create turn table by combining from and via links
#this table will serve as a lookup for links and turns
#Geoff = Turn Node
Q_CreateGeoffTable = """
    CREATE TABLE "{0}" AS (SELECT FromNode, ViaNode FROM "{1}" GROUP BY FromNode, ViaNode);
    COMMIT;
    ALTER TABLE "{0}" ADD COLUMN GeoffID SERIAL;
    COMMIT;
""".format(TBL_GEOFF_LOOKUP, TBL_SUBTURNS)
cur.execute(Q_CreateGeoffTable)

#assign geometry of via node to geoffs for calculaing closest geoff to block centroid when finding OD pairs 
Q_GeomGeoffTable = """
    CREATE TABLE "{1}" AS (
        SELECT "{0}".*, "{2}".geom FROM "{0}"
        INNER JOIN "{2}"
        ON "{2}".no = "{0}".vianode);
    COMMIT;
""".format(TBL_GEOFF_LOOKUP, TBL_GEOFF_LOOKUP_GEOM, TBL_TOLNODES)
cur.execute(Q_GeomGeoffTable)

#create master geoff (turn node) table that includes links in geoff form and turns in geoff form
#must have ID
#turns will have negative ID and links will have positive ID
Q_CreateMasterLinks = """
    CREATE TABLE "{0}" AS(
    SELECT tblA.gid AS mixID, tblA.fromgeoff, tblA.togeoff, (length*(1 + linklts)) AS cost FROM(
        SELECT
            t.*,
            g1.geoffid AS fromgeoff,
            g2.geoffid AS togeoff
        FROM (SELECT * FROM "{1}") AS t
        INNER JOIN "{3}" AS g1
        ON g1.fromnode = t.fromnodeno AND g1.vianode = t.tonodeno
        INNER JOIN "{3}" AS g2
        ON g2.fromnode = t.tonodeno AND g2.vianode = t.fromnodeno) AS tblA
    UNION 
    SELECT tblB.turnid AS mixID, tblB.fromgeoff, tblB.togeoff, cost AS cost FROM(
        SELECT
            t.*,
            g1.geoffid AS fromgeoff,
            g2.geoffid AS togeoff
        FROM (SELECT * FROM "{2}" WHERE turndirection < 4) AS t
        INNER JOIN "{3}" AS g1
        ON g1.fromnode = t.fromnode AND g1.vianode = t.vianode
        INNER JOIN "{3}" AS g2
        ON g2.fromnode = t.tonode AND g2.vianode = t.vianode) AS tblB);
    COMMIT;
""".format(TBL_MASTERLINKS, TBL_LINKS, TBL_SUBTURNS, TBL_GEOFF_LOOKUP)
cur.execute(Q_CreateMasterLinks)

#add geom colum from nodes and links to geoffs table
#must format geom columns so they are the same geometry type (linestring vs multilinestring) and both have the correct SRID
Q_Master_Geom = """
DROP TABLE IF EXISTS "{0}";
COMMIT;

CREATE TABLE "{0}" AS(
    SELECT
        tabelle0.*
    FROM (

        SELECT
            all_links.*,
            tblC.geom
        FROM (
            SELECT
                tblB.*,
                tblA.geom
            FROM (
                SELECT
                    no,
                    ST_SetSRID(ST_Multi(ST_MakeLine(pointgeom, pointgeom)), 26918) AS geom
                FROM (
                    SELECT
                        no,
                        (ST_Dump(geom)).geom AS pointgeom
                    FROM "{3}") foo
                WHERE pointgeom IS NOT NULL
                GROUP BY no, geom
            ) AS tblA
            INNER JOIN "{4}" AS tblB
            ON tblA.no = tblB.vianode
        ) AS tblC
        INNER JOIN "{1}" AS all_links
        ON all_links.mixid = tblC.turnID

        UNION ALL

        SELECT all_links.*, ST_SetSRID(ST_Multi(fil_links.geom), 26918) AS geom
        FROM "{1}" AS all_links
        INNER JOIN "{2}" AS fil_links
        ON all_links.mixid = fil_links.gid

    ) AS tabelle0
);
SELECT UpdateGeometrySRID('{0}', 'geom', 26918);
COMMIT;
""".format(TBL_MASTERLINKS_GEO, TBL_MASTERLINKS, TBL_ALL_LINKS, TBL_TOLNODES, TBL_SUBTURNS)
cur.execute(Q_Master_Geom)

######################################################################################

###############COPIED FROM 4_NetworkIslandAnalysis.py#################################
class Sponge:
    def __init__(self, *args, **kwds):
        self._args = args
        self._kwds = kwds
        for k, v in kwds.iteritems():
            setattr(self, k, v)
        self.code = 0
        self.groups = {}
        
def Group(groupNo, subgraphs, counter):
    for i, g in enumerate(subgraphs):
        for (fg, tg) in g.edges():
            if (fg, tg) in links:
                links[(fg, tg)].groups[groupNo] = counter
            if (tg, fg) in links:
                links[(tg, fg)].groups[groupNo] = counter
        counter += 1
    return counter

#grab master links to make graph with networkx
Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM "{0}";
    """.format(TBL_MASTERLINKS_GEO)
cur.execute(Q_SelectMasterLinks)
#format master links so networkx can read them
h = ['id','fg','tg','cost']
links = {}
for row in cur.fetchall():
    l = Sponge(**dict(zip(h, row)))
    links[(l.fg, l.tg)] = l

print time.ctime(), "Creating Graph"
#create graph
G = nx.MultiDiGraph()
for l in links.itervalues():
    G.add_edge(l.fg, l.tg)

print time.ctime(), "Find Islands"
counter = 0
counter = Group(0, list(nx.strongly_connected_component_subgraphs(G)), counter)
counter = Group(1, list(nx.weakly_connected_component_subgraphs(G)), counter)
counter = Group(2, list(nx.attracting_component_subgraphs(G)), counter)
counter = Group(3, list(nx.connected_component_subgraphs(G.to_undirected())), counter)

results = []
for l in links.itervalues():
    row = [l.id, l.fg, l.tg]
    for i in xrange(4):
        if i in l.groups:
            row.append(l.groups[i])
        else:
            row.append(None)
    results.append(row)

#get groups back into postgis
Q_CreateLinkGrpTable = """
    CREATE TABLE public."{0}"(
        mixid integer, 
        fromgeoff integer, 
        togeoff integer, 
        strong integer,
        weak integer,
        attracting integer,
        undirected integer
    );""".format(TBL_GROUPS)
cur.execute(Q_CreateLinkGrpTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(results[0]))))
arg_str = ','.join(cur.mogrify(str_rpl, x) for x in results)

Q_InsertGrps = """
INSERT INTO
    public."{0}"
VALUES {1}
""".format(TBL_GROUPS, arg_str)

cur.execute(Q_InsertGrps)
con.commit()
del results

#join strong and weak group number to master links geo
Q_JoinGroupGeo = """
    CREATE TABLE public."{1}" AS(
        SELECT * FROM(
            SELECT 
                t1.*,
                t0.strong,
                t0.weak
            FROM "{0}" AS t0
            LEFT JOIN "{2}" AS t1
            ON t0.mixid = t1.mixid
        ) AS "{1}"
    );""".format(TBL_GROUPS, TBL_MASTERLINKS_GROUPS, TBL_MASTERLINKS_GEO)
cur.execute(Q_JoinGroupGeo)
con.commit()
#################################################

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

###NEED TO BREAK UP INTO COUNTY TABLES FIRST###
#select top 10 percent from county lts result tables to identify priority links to work with
TBL_bucks        = "bucks_linkuse"
TBL_chester      = "chesco_linkuse"
TBL_delaware     = "delco_linkuse"
TBL_montgomery   = "montco_linkuse"
TBL_philadelphia = "phila_linkuse"

county_tbls = (TBL_bucks, TBL_chester, TBL_delaware, TBL_montgomery, TBL_philadelphia)
county_labels = ("Bucks", "Chester", "Delaware", "Montgomery", "Philadelphia")

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


###which L1&2 islands would be connected by LTS 3 prioritiy segments (top 10% only)

##SELECT ALL THE LINKS THAT ARE PART OF EACH ISLAND
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
#for testarea - will need to add county back into insert at bottom
cur.execute("""SELECT edge FROM "{0}";""".format(TBL_LTS3))
testedges = cur.fetchall()

for edge in testedges:
    
# for edge, county in priorities:
    # if county == "Bucks":
        # tbl = TBL_bucks
    # elif county == "Chester":
        # tbl = TBL_chester
    # elif county == "Delaware":
        # tbl = TBL_delaware
    # elif county == "Montgomery":
        # tbl = TBL_montgomery
    # elif county == "Philadelphia":
        # tbl = TBL_philadelphia
    
    # print edge, tbl

    cur.execute(Q_BUFFER_INTERSECT.format(TBL_LTS3, int(edge[0]), TBL_MASTERLINKS_GROUPS))
    strong = cur.fetchall()
    
    Q_SELECT_ISLANDS = """
        SELECT mixid, strong, ST_AsGeoJSON(geom)
        FROM "{0}"
        WHERE strong = {1};"""
                
    edgeList = []
    mixidList = []
    islandList = []
    geomList = []
    ###ADD BACK IN AFTER TESTING
    #countyList = []
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
            #countyList.append(county)
        elif len(strong) >= 2:
            a = strong[i][0]
            b = int(strong[i][1])
            cur.execute(Q_SELECT_ISLANDS.format(TBL_MASTERLINKS_GROUPS, a))
            links = cur.fetchall()
            for j in xrange(0, len(links)):
                edgeList.append(b)
                mixidList.append(links[j][0])
                islandList.append(links[j][1])
                geomList.append(links[j][2])
                #countyList.append(county)

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
    if len(edgeList) == len(mixidList) == len(islandList) == len(geomList): #== len(countyList):
        print "All lists are of equal length"
    else:
        print "List length mismatch"

    #geometry field will be NULL for rows where the edge does not connect multiple islands
    Q_Insert = """INSERT INTO public."{0}" (edge, mixid, island, geom) VALUES ({1},{2},{3},(ST_SetSRID(ST_GeomFromGeoJSON('{4}'), 26918)));"""
    Q_Insert_noGeom = """INSERT INTO public."{0}" (edge, mixid, island) VALUES ({1},{2},{3});"""
    # Q_Insert = """INSERT INTO public."{0}" (edge, mixid, island, counties, geom) VALUES ({1},{2},{3},'{4}',(ST_SetSRID(ST_GeomFromGeoJSON('{5}'), 26918)));"""
    # Q_Insert_noGeom = """INSERT INTO public."{0}" (edge, mixid, island, counties) VALUES ({1},{2},{3},'{4}');"""

    for i in xrange(0,len(edgeList)):
        edge = edgeList[i]
        mixid = mixidList[i]
        island = islandList[i]
        geo = geomList[i]
        #co = countyList[i]
        if geo == 0:
            cur.execute(Q_Insert_noGeom.format(TBL_CON_ISLANDS, edge, mixid, island))
        else:
            cur.execute(Q_Insert.format(TBL_CON_ISLANDS, edge, mixid, island, geo))

            
            
            
#using reslts from above, find total length of connected roads based on the connecting edge
SELECT edge, SUM(ST_Length(geom)) sumLength
FROM montco_con_islands2
GROUP BY edge
ORDER BY sumLength DESC
