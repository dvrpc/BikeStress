#copy to run in cmd
#C:\Users\model-ws\AppData\Local\Continuum\Anaconda2\python.exe D:\BikePedTransit\BikeStress\scripts\GIT\BikeStress\7_CalculateShortestPaths_PARENT_MovingFrame.py

import psycopg2 as psql
import subprocess
import time
import sys
import CalculateShortestPaths_CLEANUP as cleanup
from tqdm import tqdm

from child_movingframe_shortestpath import run_child_moving_frame
from child_movingframe_shortestpath import worker
from child_movingframe_shortestpath import test_workers
from database import connection

cur = connection.cursor()

TBL_WORK_NETWORK = "temp_network_502_%d_%d"

def run_child_script(i, j):

    this_table = TBL_WORK_NETWORK %(i, j)

    cur.execute("""SELECT EXISTS (SELECT 1 AS result FROM pg_tables WHERE schemaname = 'public' AND tablename = '{0}');""".format(this_table))
    table_exists = cur.fetchone()[0]
    if table_exists:
        cur.execute("""SELECT COUNT(*) FROM %s WHERE MIXID > 0""" % (this_table))
        cnt, = cur.fetchone()
        if cnt > 0:
            run_child_moving_frame(i, j, log=True)
            #cleanup.dumpndrop_MF(i, j)

#to finish from section 10-16 and above
if __name__ == "__main__":
    for i in xrange(1, 32):
        for j in tqdm(xrange(1, 32)):
            print i, j
            run_child_script(i, j)

        for j in tqdm(xrange(101, 132)):
            print i, j
            run_child_script(i, j)
            
    for i in xrange(101, 124):
        for j in tqdm(xrange(1, 32)):
            print i, j
            run_child_script(i, j)

        for j in tqdm(xrange(101, 132)):
            print i, j
            run_child_script(i, j)
