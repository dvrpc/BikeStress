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



#most of these are geared towards L2 for island visualization unless otherwise noted
TBL_ALL_LINKS = "links"
TBL_LINKS = "l2_tolerablelinks"
TBL_NODES = "nodes"
TBL_TOLNODES = "l2_tol_nodes"
TBL_GEOFF_LOOKUP = "l2_geoffs"
TBL_GEOFF_LOOKUP_GEOM = "l2_geoffs_viageom"
TBL_MASTERLINKS = "l2_master_links"
TBL_MASTERLINKS_GEO = "l2_master_links_geo"
TBL_MASTERLINKS_GROUPS = "l2_master_links_grp"
TBL_GROUPS = "l2_groups"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "l2_tolerableturns"

#index names
IDX_LINKS_geom = "l2tol_links_geom_idx_ta"
IDX_LINKS_value = "l2tol_links_value_idx_ta"
IDX_TOL_NODES_geom = "l2tolnodes_geom_idx_ta"
IDX_TOL_NODES_value = "l2tolnodes_value_idx_ta"



#####################COPIED FROM 3_DataSetup.py and altered to create L1&2 master links#####################
#create subset of links based on assigned LTS
##LTS 1 and 2 only

Q_LinkSubset = """
    CREATE TABLE "{0}" AS
        SELECT * FROM "{1}" WHERE linklts <= 0.3 AND linklts >= 0 AND typeno NOT IN ('11','12','13','21','22','23','81','82','83','85','86','92');;
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
    WHERE "turnlts">= 0;
    COMMIT;
    
    UPDATE "{0}"
    SET cost = (0.005*(1 + abs("turnlts")))
    WHERE "turnlts"< 0;
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
#AUGUST 14, 2020 - Updated to incorporate slope factor into link cost
Q_CreateMasterLinks = """
    CREATE TABLE "{0}" AS(
    SELECT tblA.gid AS mixID, tblA.fromgeoff, tblA.togeoff, (length*(1 + linklts + slopefac)) AS cost FROM(
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