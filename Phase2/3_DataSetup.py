##### RUN ONCE IN BEGINNING#####

import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle
import sqlite3
from collections import Counter

###ITEMS NEEDED IN DB### read in using postgis shapefile importer
###SRID = 26918
#links with LTS assigned (polyline); link length units = feet
    #when exporting directed links from Visum, iclude the following fields: 
        #No, Fromnodeno, Tonodeno, length, numlanes, bike_facility, speedtouse, typeno, linklts, circuit flag
        #turn off all units; link length should be in miles
#block centroids with weights if desired (points) 
#nodes from model(points) - only those with >0 legs

#table names
TBL_ALL_LINKS = "testarea_links"
TBL_CENTS = "blockcentroids_testarea"
TBL_LINKS = "tolerablelinks_testarea"
TBL_NODES = "testarea_nodes"
TBL_TOLNODES = "tol_nodes_testarea"
TBL_GEOFF_LOOKUP = "geoffs_testarea"
TBL_GEOFF_LOOKUP_GEOM = "geoffs_viageom_testarea"
TBL_MASTERLINKS = "master_links_testarea"
TBL_MASTERLINKS_GEO = "master_links_geo_testarea"
TBL_MASTERLINKS_GROUPS = "master_links_grp_testarea"
TBL_GROUPS = "groups_testarea"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "tolerableturns_testarea"
TBL_BRIDGES = "delawareriverbridges"

#index names
IDX_ALL_LINKS_geom = "_talinks_geom_idx"
IDX_ALL_LINKS_value = "ta_links_value_idx"
IDX_CENTS_geom = "ta_centroids_geom_idx"
IDX_CENTS_value = "ta_centroids_value_idx"
IDX_LINKS_geom = "tol_links_geom_idx_ta"
IDX_LINKS_value = "tol_links_value_idx_ta"
IDX_NODES_geom = "nodes_geom_idx_ta"
IDX_NODES_value = "nodes_value_idx_ta"
IDX_TOL_NODES_geom = "tolnodes_geom_idx_ta"
IDX_TOL_NODES_value = "tolnodes_value_idx_ta"
IDX_ALL_TURNS_values = "All_Turns_values_idx"

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#####USE FOR INDIVIDUAL LINK LTS MODIFICATION#####
'''
#create copy of links table to modify links
Q_CreateLinkCopy = """
    CREATE TABLE "{0}" AS
        SELECT * FROM "{0}";
    COMMIT;
""".format(TBL_ALL_LINKS)
cur.execute(Q_CreateLinkCopy)

#make modifications to linklts in link table
Q_ModifyLinkLTS = """
    UPDATE "{0}" SET linklts = 0.2 WHERE gid = 248;
    UPDATE "{0}" SET linklts = 0.2 WHERE gid = 249;
    UPDATE "{0}" SET linklts = 0.2 WHERE gid = 250;
    UPDATE "{0}" SET linklts = 0.2 WHERE gid = 251;
    COMMIT;
""".format(TBL_ALL_LINKS)
cur.execute(Q_ModifyLinkLTS)

#check that the update worked (in pgadmin)
#SELECT * FROM lts_assigned_link WHERE gid = 248 OR gid = 249 OR gid = 250 OR gid = 251;
'''

#index existing data to speed up processing
Q_IndexExisting = """
    CREATE INDEX IF NOT EXISTS "{2}"
        ON public."{0}" USING gist
        (geom)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS "{3}"
        ON public."{0}" USING btree
        (gid, countyfp10)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS "{4}"
        ON public."{1}" USING gist
        (geom)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS "{5}"
        ON public."{1}" USING btree
        (gid, no, fromnodeno, tonodeno, length, linklts)
        TABLESPACE pg_default;
        """.format(TBL_CENTS, TBL_ALL_LINKS, IDX_CENTS_geom, IDX_CENTS_value, IDX_ALL_LINKS_geom, IDX_ALL_LINKS_value)
cur.execute(Q_IndexExisting)


#create subset of links based on assigned LTS
##LTS 1 and 2 and 3 only

Q_LinkSubset = """
    CREATE TABLE "{0}" AS
        SELECT * FROM "{1}" WHERE linklts <= 0.6 AND linklts >= 0;
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


###TURNS and GEOFFS###

#####ONCE ALL TURNS ARE IMPORTED, DO NOT NEED TO REPEAT THIS SECTION UNLESS REPLACING TURNS#####
#query to create blank turn table
Q_CreateTurnTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
    FromNode integer,
    ViaNode integer,
    ToNode integer,
    MaxApproach float,
    TurnDirection integer,
    TurnLTS float
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (FromNode, ViaNode, ToNode, MaxApproach, TurnDirection, TurnLTS)
    TABLESPACE pg_default;
""".format(TBL_TURNS, IDX_ALL_TURNS_values)
cur.execute(Q_CreateTurnTable)
con.commit()

tbl_path = r"U:/FY2019/Transportation/TransitBikePed/BikeStressPhase2/data/IntermediateOutputs/TurnLTS_output_022819.csv"

#query to insert turns from csv into turn table
Q_INSERT = """
INSERT INTO public."{0}" VALUES (%s, %s, %s, %s, %s, %s)
""".format(TBL_TURNS)

#open table
data = []
with open(tbl_path, "rb") as io:
    r = csv.reader(io)
    header = r.next()
    for row in r: 
        data.append(row)
        
#convert to integers before inserting into database
data = zip(*data)
data[0] = map(lambda v:int(float(v)), data[0])
data[1] = map(lambda v:int(float(v)), data[1])
data[2] = map(lambda v:int(float(v)), data[2])
data[3] = map(lambda v:float(v), data[3])
data[4] = map(lambda v:int(float(v)), data[4])
data[5] = map(lambda v:float(v), data[5])
data = zip(*data)

cur.executemany(Q_INSERT, data)
con.commit()
############################################################################################

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
#this table will be used by djikstra for routing
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

#create table to hold list of block centroids within 5 miles of the delaware river bridges
Q_BridgeBuffer = """
    CREATE TABLE bridge_buffer AS(
        SELECT c.gid, c.statefp10, c.geom
        FROM "{0}" c, "{1}" b
        WHERE ST_Intersects(
            c.geom,
            ST_Transform(b.geom, 26918)
        );
    COMMIT;
    """.format(TBL_CENTS, TBL_BRIDGES)
cur.execute(Q_BridgeBuffer)


