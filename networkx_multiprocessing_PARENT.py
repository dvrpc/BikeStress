import psycopg2 as psql
import subprocess

PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe"
script = r"C:\Users\model-ws\Documents\Modeling\Projects\BikeStress\scripts\networkx_multiprocessing_CHILD.py"

con = psql.connect(database = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

VIEW = "links_l3_grp_%d"
for i in xrange(2, 1474):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (VIEW % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print VIEW % i
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()

# for i in xrange(2,5): # 1473
    # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
    # p.communicate()