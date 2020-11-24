#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\SchoolAnalysis\7_CalculateShortestPaths_PARENT_school.py

import psycopg2 as psql
import subprocess
import time
import sys
import CalculateShortestPaths_CLEANUP as cleanup


PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe "
script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\SchoolAnalysis\7_CalculateShortestPaths_CHILD_school.py"

con = psql.connect(database = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_MASTERLINKS_GROUPS = "master_links_grp"

TBL_BLOCK_NODE_GEOFF = "block_node_geoff_school"

Q_GeoffCount = """
SELECT 
    COUNT(*) AS cnt 
FROM (
    SELECT 
        fromdid, 
        togid 
    FROM "{0}" 
    WHERE groupnumber = %d AND fromdid <> togid 
    GROUP BY fromdid, togid )
    AS _q0
    """.format(TBL_BLOCK_NODE_GEOFF)



a = 0 #min island number
b = 1437 #max island number  
c= b+1 #this number is not calcualted (this is the big island in this case)
dumpers = []
for i in xrange(a, c):
    selectisland = """(SELECT * FROM {0} WHERE strong = {1})""".format(TBL_MASTERLINKS_GROUPS, i)
    cur.execute("SELECT COUNT(*) FROM {0} view WHERE MIXID > 0".format(selectisland))
    cnt, = cur.fetchone()
    if cnt > 0:
        cur.execute(Q_GeoffCount % i)
        geoffCount = cur.fetchall()
        if geoffCount > 0:
            #cur.execute("SELECT ")
            print TBL_TEMP_NETWORK % i
            p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)#call script to calculate shortest paths
            p.communicate()
            
            cleanup.dumpndrop(i)


a = 1439 #min island number
b = 14216 #max island number
c= b+1
dumpers = []
for i in xrange(a, c):
    selectisland = """(SELECT * FROM {0} WHERE strong = {1})""".format(TBL_MASTERLINKS_GROUPS, i)
    cur.execute("SELECT COUNT(*) FROM {0} view WHERE MIXID > 0".format(selectisland))
    cnt, = cur.fetchone()
    if cnt > 0:
        cur.execute(Q_GeoffCount % i)
        geoffCount = cur.fetchall()
        if geoffCount > 0:
            #cur.execute("SELECT ")
            print TBL_TEMP_NETWORK % i

            p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)#call script to calculate shortest paths
            p.communicate()
            
            cleanup.dumpndrop(i)


