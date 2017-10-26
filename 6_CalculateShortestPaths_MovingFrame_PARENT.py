import psycopg2 as psql
import subprocess
import time
import sys

PYEXE = r"C:\Users\model-ws.DVRPC_PRIMARY\AppData\Local\Continuum\Anaconda2\python.exe"
# script = r"D:\Modeling\BikeStress\scripts\test.py"
script = r"D:\Modeling\BikeStress\scripts\6_CalculateShortestPaths_MovingFrame_CHILD.py"


con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

TBL_WORK_NETWORK = "temp_network_338_%d_%d"
# TBL_TEMP_NETWORK = "temp_network_196_%s"
# TBL_TEMP_PAIRS = "temp_pairs_196_%s"
# TBL_TEMP_NETWORK = "temp_network_180_%s" % str(sys.argv[1])
# TBL_BLOCK_NODE_GEOFF = "block_node_geoff"

# Q_GeoffCount = """
# SELECT 
    # COUNT(*) AS cnt 
# FROM (
    # SELECT 
        # fromgid, 
        # togid 
    # FROM "{0}" 
    # WHERE groupnumber = %d AND fromgid <> togid 
    # GROUP BY fromgid, togid )
    # AS _q0
    # """.format(TBL_BLOCK_NODE_GEOFF)


###for views/non-split islands
# for i in xrange(1, 338):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # print TBL_TEMP_NETWORK % i
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()
        
# for i in xrange(1055, 5450):
    # cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
    # cnt, = cur.fetchone()
    # if cnt > 0:
        # cur.execute(Q_GeoffCount % i)
        # geoffCount = cur.fetchall()
        # if geoffCount > 0:
            # cur.execute("SELECT ")
            # print TBL_TEMP_NETWORK % i
            # with open("temp_processing.txt", "ab") as io:
                # io.write("{0}: {1}\r\n".format(time.ctime(), i))
            # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
            # p.communicate()

###for split islands/moving frame
for i in xrange(1, 10):
    for j in xrange(1, 12):
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_WORK_NETWORK %(i, j)))
        table_exists = cur.fetchone()[0]
        # print TBL_WORK_NETWORK % (i, j), table_exists
        if table_exists == True:
            cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (TBL_WORK_NETWORK %(i, j)))
            cnt, = cur.fetchone()
            if cnt > 0:
                print TBL_WORK_NETWORK % (i, j)
                with open("temp_processing.txt", "ab") as io:
                    io.write("{0}: {1}{2}\r\n".format(time.ctime(), i, j))
                print [PYEXE, script, '%s' % i, '%s' % j]
                p = subprocess.Popen([PYEXE, script, '%d' % i, '%d' % j], stdout = subprocess.PIPE)
                p.communicate()
    for j in xrange(101, 112):
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_WORK_NETWORK %(i, j)))
        table_exists = cur.fetchone()[0]
        # print TBL_WORK_NETWORK % (i, j), table_exists
        if table_exists == True:
            cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (TBL_WORK_NETWORK %(i, j)))
            cnt, = cur.fetchone()
            if cnt > 0:
                print TBL_WORK_NETWORK % (i, j)
                with open("temp_processing.txt", "ab") as io:
                    io.write("{0}: {1}{2}\r\n".format(time.ctime(), i, j))
                print [PYEXE, script, '%s' % i, '%s' % j]
                p = subprocess.Popen([PYEXE, script, '%d' % i, '%d' % j], stdout = subprocess.PIPE)
                p.communicate()
        
for i in xrange(101, 111):
    for j in xrange(1, 12):
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_WORK_NETWORK %(i, j)))
        table_exists = cur.fetchone()[0]
        # print TBL_WORK_NETWORK % (i, j), table_exists
        if table_exists == True:
            cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (TBL_WORK_NETWORK %(i, j)))
            cnt, = cur.fetchone()
            if cnt > 0:
                print TBL_WORK_NETWORK % (i, j)
                with open("temp_processing.txt", "ab") as io:
                    io.write("{0}: {1}{2}\r\n".format(time.ctime(), i, j))
                print [PYEXE, script, '%s' % i, '%s' % j]
                p = subprocess.Popen([PYEXE, script, '%d' % i, '%d' % j], stdout = subprocess.PIPE)
                p.communicate()
    for j in xrange(101, 112):
        cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(TBL_WORK_NETWORK %(i, j)))
        table_exists = cur.fetchone()[0]
        # print TBL_WORK_NETWORK % (i, j), table_exists
        if table_exists == True:
            cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (TBL_WORK_NETWORK %(i, j)))
            cnt, = cur.fetchone()
            if cnt > 0:
                print TBL_WORK_NETWORK % (i, j)
                with open("temp_processing.txt", "ab") as io:
                    io.write("{0}: {1}{2}\r\n".format(time.ctime(), i, j))
                print [PYEXE, script, '%s' % i, '%s' % j]
                p = subprocess.Popen([PYEXE, script, '%d' % i, '%d' % j], stdout = subprocess.PIPE)
                p.communicate()
