#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\TransitAnalysis\7_CalculateShortestPaths_PARENT_MovingFrame_transit_10.py

import psycopg2 as psql
import subprocess
import time
import sys
import CalculateShortestPaths_CLEANUP as cleanup

PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe "
# script = r"D:\Modeling\BikeStress\scripts\test.py"
script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\TransitAnalysis\7_CalculateShortestPaths_CHILD_MovingFrame_transit.py"


con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_WORK_NETWORK = "temp_network_332_%d_%d"


###for split islands/moving frame
for i in xrange(10, 11):
    for j in xrange(0, 1):
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_WORK_NETWORK %(i, j)))
        table_exists = cur.fetchone()[0]
        # print TBL_WORK_NETWORK % (i, j), table_exists
        if table_exists == True:
            cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (TBL_WORK_NETWORK %(i, j)))
            cnt, = cur.fetchone()
            if cnt > 0:
                print TBL_WORK_NETWORK % (i, j)
                p = subprocess.Popen([PYEXE, script, '%d' % i, '%d' % j], stdout = subprocess.PIPE)
                p.communicate()
                
                cleanup.dumpndrop_MF(i, j)
   
               