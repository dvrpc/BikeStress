import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle
import sqlite3
from collections import Counter


#create table to hold turns
TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco__L3_tolerablelinks"
TBL_NODES = "montco_nodes"
TBL_SPATHS = "montco_L3_shortestpaths"
TBL_TOLNODES = "montco__L3_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_L3_geoffs"
TBL_GEOFF_LOOKUP_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_L3_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_GROUPS = "montco_L3_groups"
TBL_TURNS = "all_turns"
TBL_SUBTURNS = "montco_L3_turns"

IDX_ALL_TURNS_values = "All_Turns_values_idx"

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#ONCE ALL TURNS ARE IMPORTED, DO NOT NEED TO REPEAT THIS UNLESS REPLACING TURNS

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

tbl_path = r"//peach/Modeling/Projects/BikeStress/TurnLTS_output.csv"

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
    SELECT tblA.gid AS mixID, tblA.fromgeoff, tblA.togeoff, CAST(trim(trailing 'mi' FROM "length") AS float)* (1 + "linklts") AS cost FROM(
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
                    ST_SetSRID(ST_Multi(ST_MakeLine(geom, geom)), 26918) AS geom
                FROM "{3}"
                WHERE geom IS NOT NULL
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
    

#assign geom to geoffs by first assigning to turns and joining to turns and links (as temp table)
#join it to edge count
#create a table from this and/or view in qgis
#WITH tblD AS (
#    SELECT * FROM(
#        SELECT eg_master_links.*, tblC.geom FROM(
#            SELECT tblB.*, tblA.st_makeline AS geom FROM (
#                SELECT no, ST_MakeLine(geom, geom) FROM "eg_nodes" WHERE geom IS NOT NULL GROUP BY no, geom
#            ) AS tblA
#            INNER JOIN eg_turns AS tblB
#            ON tblA.no = tblB.vianode
#        ) AS tblC
#        INNER JOIN eg_master_links
#        ON eg_master_links.mixid = tblC.turnID
#
#        UNION ALL
#
#        SELECT eg_master_links.*, eg_lts_links.geom FROM eg_master_links
#        INNER JOIN eg_lts_links
#        ON eg_master_links.mixid = eg_lts_links.gid
#    ) AS tblD
#)
#SELECT 
#    tblD.*,
#    tbl10.cnt as cnt
#FROM(
#    SELECT 
#        edge,
#        COUNT(*) AS cnt
#    FROM eg_shortestpaths
#    GROUP BY edge
#) AS tbl10
#INNER JOIN tblD
#ON tblD.mixid = tbl10.edge;
