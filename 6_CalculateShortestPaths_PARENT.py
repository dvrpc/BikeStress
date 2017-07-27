import psycopg2 as psql
import subprocess
import time
import sys

PYEXE = r"C:\Users\model-ws.DVRPC_PRIMARY\AppData\Local\Continuum\Anaconda2\python.exe"
script = r"D:\Modeling\BikeStress\scripts\6_CalculateShortestPaths_CHILD.py"

con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_TEMP_NETWORK = "links_l3_grp_%s"
# TBL_TEMP_NETWORK = "temp_network_196_%s"
# TBL_TEMP_PAIRS = "temp_pairs_196_%s"
# TBL_TEMP_NETWORK = "temp_network_180_%s" % str(sys.argv[1])

###for views/non-split islands
for i in xrange(1, 338):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print TBL_TEMP_NETWORK % i
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()
        
for i in xrange(339, 5450):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print TBL_TEMP_NETWORK % i
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()

###for split islands/moving frame
# for i in xrange(1, 4):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # print TBL_TEMP_NETWORK % i
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # print [PYEXE, script, '%d' % i]
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()
        
# for i in xrange(101, 105):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # print TBL_TEMP_NETWORK % i
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()

#old?
# for i in xrange(2,5): # 1473
    # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
    # p.communicate()