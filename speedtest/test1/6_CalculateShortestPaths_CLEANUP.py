import os
import subprocess
import sys
import psycopg2 as psql

TBL_SPATHS = "shortestpaths_%s" % str(sys.argv[1])

p = subprocess.Popen(
    [
        r"C:\Program Files (x86)\pgAdmin III\1.22\pg_dump.exe", 
        "--host", "localhost", 
        "--port", "5432", 
        "--username", "postgres", 
        "--no-password", 
        "--format", "tar", 
        "--verbose", 
        "--file", r"M:\Modeling\Projects\BikeStress_p2\%s.backup" % TBL_SPATHS, 
        "--table", "public.%s" % TBL_SPATHS,
        "BikeStress_p2"],
    stdout = subprocess.PIPE, 
    stderr = subprocess.PIPE) 

stdout, stderr = p.communicate()


con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


cur.execute("DROP TABLE public.%s;"% (TBL_SPATHS))
con.commit()

###dump to modeling or external drive (faster)

