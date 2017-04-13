##### RUN ONCE IN BEGINNING#####

import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle

###ITEMS NEEDED IN DB###
#links with LTS assigned (polyline)
#block centroids with weights if desired (points)

#table names
TBL_ALL_LINKS = "mercer_assigned_improved"
TBL_CENTS = "mercer_centroids"
TBL_LINKS = "mercer_tolerablelinks_improved"
TBL_NODES = "mercer_nodes_improved"
TBL_SPATHS = "mercer_shortestpaths_improved"
#index names
IDX_ALL_LINKS_geom = "mercer_links_geom_idx_improved"
IDX_ALL_LINKS_value = "mercer_links_value_idx_improved"
IDX_CENTS_geom = "mercer_centroids_geom_idx"
IDX_CENTS_value = "mercer_centroids_value_idx"
IDX_LINKS_geom = "mercer_tol_links_geom_idx_improved"
IDX_LINKS_value = "mercer_tol_links_value_idx_improved"
IDX_SPATHS_value = "mercer_spaths_value_idx_improved"
IDX_NODES_geom = "mercer_nodes_geom_idx"
IDX_NODES_value = "mercer_nodes_value_idx"

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
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
#LTS 1 and 2 only
Q_LinkSubset = """
    CREATE TABLE "{0}" AS
        SELECT * FROM "{1}" WHERE linklts <= 0.3 AND linklts > 0;
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

#create start end end point nodes for all links
#need to convert from multilinestring to linesting with collection homogenize
#union the start points and end points together into one table of points
Q_StartEndPoints = """
CREATE TABLE "{0}" AS

SELECT type, nodeno, geom FROM (

SELECT 1 AS type, fromnodeno AS nodeno, geom FROM (
    SELECT
        fromnodeno,
        ST_StartPoint(ST_CollectionHomogenize(geom)) AS geom
    FROM public."{1}"
) AS tblA

UNION ALL

SELECT 2 AS type, tonodeno AS nodeno, geom FROM (
    SELECT
        tonodeno,
        ST_EndPoint(ST_CollectionHomogenize(geom)) AS geom
    FROM public."{1}"
) AS tblB

) AS "{0}";
CREATE INDEX IF NOT EXISTS "{2}"
    ON public."{0}" USING gist
    (geom)
    TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS "{3}"
    ON public."{0}" USING btree
    (nodeno)
    TABLESPACE pg_default;    
""".format(TBL_NODES, TBL_LINKS, IDX_NODES_geom, IDX_NODES_value)
cur.execute(Q_StartEndPoints)
cur.execute("COMMIT;")

#query to create table to hold shortest paths
Q_CreatePathTable = """
    CREATE TABLE IF NOT EXISTS public."{0}"
    (
        sequence integer,
        ogid integer,
        dgid integer,
        seq integer,
        path_seq integer,
        node bigint,
        edge bigint,
        cost double precision,
        agg_cost double precision
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;
    COMMIT;
    
    CREATE INDEX IF NOT EXISTS "{1}"
        ON public."{0}" USING btree
        (sequence, ogid, dgid, seq, path_seq, node, edge, cost, agg_cost)
        TABLESPACE pg_default;    
""".format(TBL_SPATHS, IDX_SPATHS_value)
cur.execute(Q_CreatePathTable)