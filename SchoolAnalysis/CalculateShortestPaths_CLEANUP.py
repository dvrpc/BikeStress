import psycopg2 as psql
import os
import subprocess
import sys


def dumpndrop(num):

    TBL_SPATHS = "shortestpaths_%s" % num
    
    con = psql.connect(database = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    #does the table exist?
    cur.execute("""SELECT EXISTS (
                    SELECT 1
                    FROM   information_schema.tables 
                    WHERE  table_schema = 'public'
                    AND    table_name = '%s');""" % (TBL_SPATHS))
    result = cur.fetchone()

    if result[0] == True:
    #is there anything in the table?
        cur.execute("SELECT COUNT(*) FROM %s" % (TBL_SPATHS))
        cnt, = cur.fetchone()

        if cnt > 0:
            #backup table
            p = subprocess.Popen(
                [
                    r"C:\Program Files (x86)\pgAdmin III\1.22\pg_dump.exe", 
                    "--host", "localhost", 
                    "--port", "5432", 
                    "--username", "postgres", 
                    "--no-password", 
                    "--format", "tar", 
                    "--verbose", 
                    "--file", r"D:\BikePedTransit\BikeStress\phase3\backups\%s.backup" % TBL_SPATHS, 
                    "--table", "public.%s" % TBL_SPATHS,
                    "BikeStress_p3"],
                stdout = subprocess.PIPE, 
                stderr = subprocess.PIPE) 

            stdout, stderr = p.communicate()
            
            #drop table
            cur.execute("DROP TABLE IF EXISTS public.%s;"% (TBL_SPATHS))
            con.commit()

        else:
            cur.execute("DROP TABLE IF EXISTS public.%s;"% (TBL_SPATHS))
            con.commit()




if __name__ == "__main__":
    dumpndrop(num)

#moving frame version of function
def dumpndrop_MF(num1, num2):

    TBL_SPATHS = "shortestpaths_%s_%s" % (num1, num2)
    
    con = psql.connect(database = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
    cur = con.cursor()

    #does the table exist?
    cur.execute("""SELECT EXISTS (
                    SELECT 1
                    FROM   information_schema.tables 
                    WHERE  table_schema = 'public'
                    AND    table_name = '%s');""" % (TBL_SPATHS))
    result = cur.fetchone()

    if result[0] == True:
    #is there anything in the table?
        cur.execute("SELECT COUNT(*) FROM %s" % (TBL_SPATHS))
        cnt, = cur.fetchone()

        if cnt > 0:
            #backup table
            p = subprocess.Popen(
                [
                    r"C:\Program Files (x86)\pgAdmin III\1.22\pg_dump.exe", 
                    "--host", "localhost", 
                    "--port", "5432", 
                    "--username", "postgres", 
                    "--no-password", 
                    "--format", "tar", 
                    "--verbose", 
                    "--file", r"D:\BikePedTransit\BikeStress\phase3\backups\%s.backup" % TBL_SPATHS, 
                    "--table", "public.%s" % TBL_SPATHS,
                    "BikeStress_p3"],
                stdout = subprocess.PIPE, 
                stderr = subprocess.PIPE) 

            stdout, stderr = p.communicate()
            
            #drop table
            cur.execute("DROP TABLE IF EXISTS public.%s;"% (TBL_SPATHS))
            con.commit()

        else:
            cur.execute("DROP TABLE IF EXISTS public.%s;"% (TBL_SPATHS))
            con.commit()




if __name__ == "__main__":
    dumpndrop_MF(num1, num2)    