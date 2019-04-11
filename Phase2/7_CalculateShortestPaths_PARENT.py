#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\7_CalculateShortestPaths_PARENT.py

import psycopg2 as psql
import subprocess
import time
import sys

PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe "
script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\7_CalculateShortestPaths_CHILD.py"
cleanup_script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\CalculateShortestPaths_CLEANUP.py"

con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_TEMP_NETWORK = "links_grp_%s"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"

Q_GeoffCount = """
SELECT 
    COUNT(*) AS cnt 
FROM (
    SELECT 
        fromgid, 
        togid 
    FROM "{0}" 
    WHERE groupnumber = %d AND fromgid <> togid 
    GROUP BY fromgid, togid )
    AS _q0
    """.format(TBL_BLOCK_NODE_GEOFF)


a = 0 #min island number
b = 331 #max island number  
c= b+1 #this number is not calcualted (this is the big island in this case)
dumpers = []
for i in xrange(a, c):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
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


a = 333 #min island number
b = 7880 #max island number
c= b+1
dumpers = []
for i in xrange(a, c):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
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