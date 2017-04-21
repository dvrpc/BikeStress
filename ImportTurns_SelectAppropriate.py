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
TBL_TURNS = "All_Turns"
TBL_ALL_LINKS = "eg_lts_links"
TBL_SUBTURNS = "eg_turns"
TBL_GEOFFS = "geoffs"

IDX_ALL_TURNS_values = "All_Turns_values_idx"

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

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

#create unique ID field for turns
Q_AddID = """
    ALTER TABLE public."{0}"
    ADD ID SERIAL;
""".format(TBL_TURNS)
cur.execute(Q_AddID)
con.commit()

    
#query to select turns that share a node with the links in the subset of links being used
Q_SubsetTurns = """
CREATE TABLE "{2}" AS(
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
    );""".format(TBL_TURNS, TBL_ALL_LINKS, TBL_SUBTURNS)
cur.execute(Q_SubsetTurns)
con.commit()

#query to create turn table by combining from and via links
#this table will serve as a lookup for links and turns
#Geoff = Turn Node
Q_CreateGeoffTable = """
    CREATE TABLE "{0}" AS (SELECT FromNode, ViaNode FROM "{1}" GROUP BY FromNode, ViaNode);
    COMMIT;
    ALTER TABLE "{0}" ADD COLUMN GeoffID SERIAL;
    COMMIT;
""".format(TBL_GEOFFS, TBL_SUBTURNS)
cur.execute(Q_CreateGeoffTable)

#need to create singluar cost field for links and turns
#raise limit on shortest paths to include to 20 to account for the added costs of turns?


Q_LinkGeoffs = """
    SELECT
        t.*,
        g1.geoffid AS fromgeoff,
        g2.geoffid AS togeoff
    FROM (SELECT * FROM "{1}" LIMIT 10) AS t
    INNER JOIN "{0}" AS g1
    ON g1.fromnode = t.fromnodeno AND g1.vianode = t.tonodeno
    INNER JOIN "{0}" AS g2
    ON g2.fromnode = t.tonodeno AND g2.vianode = t.fromnodeno;
""".format(TBL_GEOFFS, TBL_ALL_LINKS)
cur.execute(Q_LinkGeoffs)


ALTER TABLE eg_turns ADD COLUMN dummy SERIAL;
ALTER TABLE eg_turns ADD COLUMN turnID integer;
UPDATE eg_turns
SET turnID = dummy*-1;
COMMIT;

#taking forever and doesn't really matter so just left dummy column in
ALTER TABLE public."eg_turns" DROP COLUMN dummy;
COMMIT;

#create master geoff (turn node) table that includes links in geoff form and turns in geoff form
#this table will be used by djikstra for routing
#must have ID
#turns will have negative ID and links will have positive ID

CREATE TABLE eg_geoffs AS(
SELECT tblA.gid AS mixID, tblA.fromgeoff, tblA.togeoff, CAST(trim(trailing 'mi' FROM "length") AS float)* (1 + "linklts") AS cost FROM(
    -- LINKS
    SELECT
        t.*,
        g1.geoffid AS fromgeoff,
        g2.geoffid AS togeoff
    FROM (SELECT * FROM eg_lts_links) AS t
    INNER JOIN geoffs AS g1
    ON g1.fromnode = t.fromnodeno AND g1.vianode = t.tonodeno
    INNER JOIN geoffs AS g2
    ON g2.fromnode = t.tonodeno AND g2.vianode = t.fromnodeno) AS tblA
UNION 
SELECT tblB.turnID AS mixID, tblB.fromgeoff, tblB.togeoff, tblB.turnlts AS cost FROM(
    -- TURNS
    SELECT
        t.*,
        g1.geoffid AS fromgeoff,
        g2.geoffid AS togeoff
    FROM (SELECT * FROM eg_turns WHERE turndirection < 4) AS t
    INNER JOIN geoffs AS g1
    ON g1.fromnode = t.fromnode AND g1.vianode = t.vianode
    INNER JOIN geoffs AS g2
    ON g2.fromnode = t.tonode AND g2.vianode = t.vianode) AS tblB);
COMMIT;

    
-- SELECT * FROM (SELECT nodeno, COUNT(*) AS cnt FROM (SELECT nodeno, geom FROM "eg_nodes" WHERE geom IS NOT NULL GROUP BY nodeno, geom) AS t GROUP BY nodeno) AS tt WHERE cnt = 2;
#find create "lines" for turns from the point to the same point
#will give them a geom
SELECT nodeno, ST_MakeLine(geom, geom) FROM "eg_nodes" WHERE geom IS NOT NULL GROUP BY nodeno, geom;

#assign geom to geoffs by first assigning to turns and joining to turns and links (as temp table)
#join it to edge count
#create a table from this and/or view in qgis
WITH tblD AS (
    SELECT * FROM(
        SELECT eg_geoffs.*, tblC.geom FROM(
            SELECT tblB.*, tblA.st_makeline AS geom FROM (
                SELECT no, ST_MakeLine(geom, geom) FROM "eg_nodes" WHERE geom IS NOT NULL GROUP BY no, geom
            ) AS tblA
            INNER JOIN eg_turns AS tblB
            ON tblA.no = tblB.vianode
        ) AS tblC
        INNER JOIN eg_geoffs
        ON eg_geoffs.mixid = tblC.turnID

        UNION ALL

        SELECT eg_geoffs.*, eg_lts_links.geom FROM eg_geoffs
        INNER JOIN eg_lts_links
        ON eg_geoffs.mixid = eg_lts_links.gid
    ) AS tblD
)
SELECT 
    tblD.*,
    tbl10.cnt as cnt
FROM(
    SELECT 
        edge,
        COUNT(*) AS cnt
    FROM eg_shortestpaths
    GROUP BY edge
) AS tbl10
INNER JOIN tblD
ON tblD.mixid = tbl10.edge;




        
SELECT * FROM pgr_dijkstra(
    'SELECT mixid AS id, fromgeoff AS source, togeoff AS target, cost AS cost FROM "eg_geoffs"', 
    795, 1713);