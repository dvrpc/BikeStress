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

####table names to modify in subsequent runs###
#lts_assigned_link / delco_ltslinks / mercer_assigned_links
#tolerable_links / delco_tolerablelinks / mercer_tolerablelinks
#nodes / delco_nodes /mercer_nodes
#shortest_paths_delco / mercer_shortestpaths
#delco_blockcentroids / mercer_centroids
#CHANGE ALL INDEX NAMES


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#####USE FOR INDIVIDUAL LINK LTS MODIFICATION#####
'''
#create copy of links table to modify links
Q_CreateLinkCopy = """
    CREATE TABLE lts_assigned_link AS
        SELECT * FROM lts_assigned_link;
    COMMIT;
"""
cur.execute(Q_CreateLinkCopy)

#make modifications to linklts in link table
Q_ModifyLinkLTS = """
    UPDATE lts_assigned_link SET linklts = 0.2 WHERE gid = 248;
    UPDATE lts_assigned_link SET linklts = 0.2 WHERE gid = 249;
    UPDATE lts_assigned_link SET linklts = 0.2 WHERE gid = 250;
    UPDATE lts_assigned_link SET linklts = 0.2 WHERE gid = 251;
    COMMIT;
"""
cur.execute(Q_ModifyLinkLTS)

#check that the update worked (in pgadmin)
#SELECT * FROM lts_assigned_link WHERE gid = 248 OR gid = 249 OR gid = 250 OR gid = 251;
'''

#index existing data to speed up processing
Q_IndexExisting = """
    CREATE INDEX IF NOT EXISTS mercer_blockcentroids_geom_idx
        ON public.mercer_centroids USING gist
        (geom)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS mercer_blockcentroids_value_idx
        ON public.mercer_centroids USING btree
        (gid, countyfp10)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS mercer_ltslinks_geom_idx
        ON public.mercer_assigned_links USING gist
        (geom)
        TABLESPACE pg_default;
    CREATE INDEX IF NOT EXISTS mercer_ltslinks_value_idx
        ON public.mercer_assigned_links USING btree
        (gid, no, fromnodeno, tonodeno, length, linklts)
        TABLESPACE pg_default;
        """
cur.execute(Q_IndexExisting)


#create subset of links based on assigned LTS
#LTS 1 and 2 only
Q_LinkSubset = """
    CREATE TABLE mercer_tolerablelinks AS
        SELECT * FROM "mercer_assigned_links" WHERE linklts <= 0.3 AND linklts > 0;
    COMMIT;
    CREATE INDEX IF NOT EXISTS mercer_tolerablelinks_geom_idx
        ON public.mercer_tolerablelinks USING gist
        (geom)
        TABLESPACE pg_default; 
    CREATE INDEX IF NOT EXISTS mercer_tolerablelinks_value_idx
        ON public.mercer_tolerablelinks USING btree
        (gid, no, fromnodeno, tonodeno, length, linklts)
        TABLESPACE pg_default;    
"""
cur.execute(Q_LinkSubset)

#create start end end point nodes for all links
#need to convert from multilinestring to linesting with collection homogenize
#union the start points and end points together into one table of points
Q_StartEndPoints = """
CREATE TABLE mercer_nodes AS

SELECT type, nodeno, geom FROM (

SELECT 1 AS type, fromnodeno AS nodeno, geom FROM (
    SELECT
        fromnodeno,
        ST_StartPoint(ST_CollectionHomogenize(geom)) AS geom
    FROM public."mercer_tolerablelinks"
) AS tblA

UNION ALL

SELECT 2 AS type, tonodeno AS nodeno, geom FROM (
    SELECT
        tonodeno,
        ST_EndPoint(ST_CollectionHomogenize(geom)) AS geom
    FROM public."mercer_tolerablelinks"
) AS tblB

) AS mercer_nodes;
CREATE INDEX IF NOT EXISTS mercer_nodes_geom_idx
    ON public.mercer_nodes USING gist
    (geom)
    TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS mercer_nodes_value_idx
    ON public.mercer_nodes USING btree
    (nodeno)
    TABLESPACE pg_default;    
"""
cur.execute(Q_StartEndPoints)
cur.execute("COMMIT;")

#query to create table to hold shortest paths
Q_CreatePathTable = """
    CREATE TABLE IF NOT EXISTS public.mercer_shortestpaths
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
    
    CREATE INDEX IF NOT EXISTS mercer_shortest_paths_value_idx
        ON public.mercer_shortestpaths USING btree
        (sequence, ogid, dgid, seq, path_seq, node, edge, cost, agg_cost)
        TABLESPACE pg_default;    
"""
cur.execute(Q_CreatePathTable)