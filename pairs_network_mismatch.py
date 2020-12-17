import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle
import sqlite3
from collections import Counter

con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


countfromgeoff = """
    SELECT COUNT(*)
    FROM "{0}"
    WHERE fromgeoff IN
        (SELECT fromgeoff
        FROM "{1}")
    OR fromgeoff IN
        (SELECT togeoff
        FROM "{1}")"""

counttogeoff = """
    SELECT COUNT(*)
    FROM "{0}"
    WHERE togeoff IN
        (SELECT fromgeoff
        FROM "{1}")
    OR fromgeoff IN
        (SELECT togeoff
        FROM "{1}")"""

countpairs = """
    SELECT COUNT(*)
    FROM "{0}";
    """

def missingdatafinder(i, j):
    cur.execute(countpairs.format(TBL_P, TBL_N))
    numpairs = int(cur.fetchone()[0])
    cur.execute(countfromgeoff.format(TBL_P, TBL_N))
    countfrom = int(cur.fetchone()[0])
    cur.execute(counttogeoff.format(TBL_P, TBL_N))
    countto = int(cur.fetchone()[0])
    if numpairs == countfrom == countto:
        pass
    else:
        print i, j, "has missing data"

for i in xrange(1, 32):
    for j in xrange(1, 32):
        TBL_N = "temp_network_502_%s_%s" % (i, j)
        TBL_P = "temp_pairs_502_%s_%s" % (i, j)
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_N))
        table_exists = cur.fetchone()[0]
        if table_exists:
            missingdatafinder(i, j)

for i in xrange(1, 32):
    for j in xrange(101, 132):
        TBL_N = "temp_network_502_%s_%s" % (i, j)
        TBL_P = "temp_pairs_502_%s_%s" % (i, j)
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_N))
        table_exists = cur.fetchone()[0]
        if table_exists:
            missingdatafinder(i, j)

for i in xrange(101, 132):
    for j in xrange(1, 32):
        TBL_N = "temp_network_502_%s_%s" % (i, j)
        TBL_P = "temp_pairs_502_%s_%s" % (i, j)
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_N))
        table_exists = cur.fetchone()[0]
        if table_exists:
            missingdatafinder(i, j)

for i in xrange(101, 132):
    for j in xrange(101, 132):
        TBL_N = "temp_network_502_%s_%s" % (i, j)
        TBL_P = "temp_pairs_502_%s_%s" % (i, j)
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_N))
        table_exists = cur.fetchone()[0]
        if table_exists:
            missingdatafinder(i, j)
