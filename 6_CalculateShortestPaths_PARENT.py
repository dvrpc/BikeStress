import psycopg2 as psql
import subprocess
import time
import sys

PYEXE = r"C:\Users\model-ws.DVRPC_PRIMARY\AppData\Local\Continuum\Anaconda2\python.exe"
script = r"D:\Modeling\BikeStress\scripts\6_CalculateShortestPaths_MovingFrame_CHILD.py"

con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_TEMP_NETWORK = "temp2_network_180_%s"
# TBL_TEMP_NETWORK = "temp_network_180_%s" % str(sys.argv[1])

# for i in xrange(2, 7):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # print TBL_TEMP_NETWORK % i
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()

for i in xrange(3, 7):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print TBL_TEMP_NETWORK % i
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()
        
for i in xrange(101, 108):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print TBL_TEMP_NETWORK % i
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()

# for i in xrange(2,5): # 1473
    # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
    # p.communicate()