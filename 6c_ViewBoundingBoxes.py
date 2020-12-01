import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import sqlite3
from collections import Counter
import json


#connect to DB
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

# cur.execute("""SELECT table_name
# FROM information_schema.tables
# WHERE table_name LIKE '%temp_network_502%';""")
# tables = cur.fetchall()
# table_list = []
# for item in tables:
    # table_list.append(item)

Q_tableexist = ("""SELECT EXISTS(
            SELECT FROM pg_tables
            WHERE schemaname ='public'
            AND tablename = '{0}'
            );""")

Q_boundingbox = ("""SELECT ST_SetSRID(st_extent(geom), 26918) as extent
FROM {0};""")

chunkname = []
extent = []
for i in xrange(1,33):
    for j in xrange(1,33):
        tablename = 'temp_network_502_%s_%s' % (i, j)
        print tablename
        cur.execute(Q_tableexist.format(tablename))
        exist = cur.fetchone()
        #print exist[0]
        if exist[0] == True:
            chunkname.append(tablename)
            cur.execute(Q_boundingbox.format(tablename))
            box = cur.fetchone()
            extent.append(box[0])
            
    for j in xrange(101,133):
        tablename = 'temp_network_502_%s_%s' % (i, j)
        print tablename
        cur.execute(Q_tableexist.format(tablename))
        exist = cur.fetchone()
        #print exist[0]
        if exist[0] == True:
            chunkname.append(tablename)
            cur.execute(Q_boundingbox.format(tablename))
            box = cur.fetchone()
            extent.append(box[0])
            
for i in xrange(101,133):
    for j in xrange(1,33):
        tablename = 'temp_network_502_%s_%s' % (i, j)
        print tablename
        cur.execute(Q_tableexist.format(tablename))
        exist = cur.fetchone()
        #print exist[0]
        if exist[0] == True:
            chunkname.append(tablename)
            cur.execute(Q_boundingbox.format(tablename))
            box = cur.fetchone()
            extent.append(box[0])
            
    for j in xrange(101,133):
        tablename = 'temp_network_502_%s_%s' % (i, j)
        print tablename
        cur.execute(Q_tableexist.format(tablename))
        exist = cur.fetchone()
        #print exist[0]
        if exist[0] == True:
            chunkname.append(tablename)
            cur.execute(Q_boundingbox.format(tablename))
            box = cur.fetchone()
            extent.append(box[0])
            
output = zip(chunkname, extent)

TBL_BOX = 'chunkboxes'

Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(
  chunk character varying(50),
  geom geometry(Geometry,26918)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;
""".format(TBL_BOX)
cur.execute(Q_CreateOutputTable)

TBL_BOX = 'chunkboxes'
for i in xrange(0, len(chunkname)):
    a = chunkname[i]
    b = extent[i]
    Q_Insert = """INSERT INTO public."{0}"(chunk, geom) VALUES ('{1}', (ST_GeomFromEWKT('{2}')))"""
    cur.execute(Q_Insert.format(TBL_BOX, a, b))
con.commit()