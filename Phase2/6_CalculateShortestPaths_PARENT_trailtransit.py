#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\6_CalculateShortestPaths_PARENT.py

import psycopg2 as psql
import subprocess
import time
import sys

PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe "
script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\6_CalculateShortestPaths_CHILD_trailtransit.py"
cleanup_script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\Phase2\6_CalculateShortestPaths_CLEANUP.py"

con = psql.connect(database = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_TEMP_NETWORK = "links_grp_%s"
# TBL_TEMP_NETWORK = "temp_network_196_%s"
# TBL_TEMP_PAIRS = "temp_pairs_196_%s"
# TBL_TEMP_NETWORK = "temp_network_180_%s" % str(sys.argv[1])
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_testarea"

Q_GeoffCount = """
SELECT 
    COUNT(*) AS cnt 
FROM (
    SELECT 
        fromtid, 
        togid 
    FROM "{0}" 
    WHERE groupnumber = %d AND fromtid <> togid 
    GROUP BY fromtid, togid )
    AS _q0
    """.format(TBL_BLOCK_NODE_GEOFF)


#######for views/non-split islands#########################
# for i in xrange(1, 338):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # print TBL_TEMP_NETWORK % i
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()

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
            cur.execute("SELECT ")
            print TBL_TEMP_NETWORK % i
            with open("temp_processing.txt", "ab") as io:
                io.write("{0}: {1}\r\n".format(time.ctime(), i))
            p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)#call script to calculate shortest paths
            p.communicate()
            _p = subprocess.Popen([PYEXE, cleanup_script, '%d' % i], stdout = subprocess.PIPE) #call script to dump and delete tables
            dumpers.append(_p)

for p in dumpers:
    p.communicate()

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
            cur.execute("SELECT ")
            print TBL_TEMP_NETWORK % i
            with open("temp_processing.txt", "ab") as io:
                io.write("{0}: {1}\r\n".format(time.ctime(), i))
            p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)#call script to calculate shortest paths
            p.communicate()
            _p = subprocess.Popen([PYEXE, cleanup_script, '%d' % i], stdout = subprocess.PIPE) #call script to dump and delete tables
            dumpers.append(_p)

for p in dumpers:
    p.communicate()


