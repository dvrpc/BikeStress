#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\TransitAnalysis\7_CalculateShortestPaths_PARENT_transit.py

import psycopg2 as psql
import subprocess
import time
import sys

from database import connection

PYEXE = r"C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe "
script = r"D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\TransitAnalysis\7_CalculateShortestPaths_CHILD_transit.py"

cur = connection.cursor()

TBL_MASTERLINKS_GROUPS = "master_links_grp"
####CHANGE FOR EACH TRANSIT MODE####
string = "trolley"
TBL_BLOCK_NODE_GEOFF = "block_node_geoff_%s" %string

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

Q_SelectGroups = """
SELECT DISTINCT(groupnumber) FROM "{0}"
""".format(TBL_BLOCK_NODE_GEOFF)
cur.execute(Q_SelectGroups)
groupstouse = cur.fetchall()
print "Number of groups: ", len(groupstouse)


for item in groupstouse:
    selectisland = """(SELECT * FROM {0} WHERE strong = {1})""".format(TBL_MASTERLINKS_GROUPS, item[0])
    cur.execute("SELECT COUNT(*) FROM %s s WHERE MIXID > 0" % (selectisland))
    cnt, = cur.fetchone()
    if cnt > 0:
        cur.execute(Q_GeoffCount % item[0])
        geoffCount = cur.fetchall()
        if int(geoffCount[0][0]) > 0:
            print item[0] 
            p = subprocess.Popen([PYEXE, script, '%d' % item[0]], stdout = subprocess.PIPE)#call script to calculate shortest paths
            p.communicate()
        else:
            print "no ", item[0]
    else:
        print "no ", item[0]


