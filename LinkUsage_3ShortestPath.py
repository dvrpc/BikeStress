###RUN THROUGH BATCH###

import psycopg2 as psql # PostgreSQL connector
import csv
import itertools
import numpy
import time
import sys
import pickle


####table names to modify in subsequent runs###
#tolerable_links/ delco_tolerablelinks / mercer_tolerablelinks
#mercer_shortestpaths


#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress", host = "yoshi", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()


#query to find shortest path; one to one
#link cost is calculated on the fly by multiplying link length by (1+LinkLTS)
#make sure all trails/off road bike facilities have LinkLTS of 0
Q_ShortestPath = """
    SELECT %d AS sequence, %d AS oGID, %d AS dGID, * FROM pgr_dijkstra(
        'SELECT gid AS id, fromnodeno AS source, tonodeno AS target, (CAST(trim(trailing ''mi'' FROM "length") AS float)* (1 + "linklts")) AS cost FROM "mercer_tolerablelinks"', 
        %d, %d
    );
"""

#query to insert the resulting values of the shortest path search (6 columns for output values plus additional values for sequence counter and weights)
Q_InsertShortestPath = """
    INSERT INTO mercer_shortestpaths VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

#query to find next OD pair in table that has not been run yet
Q_SelectODpair = """
    SELECT ogid AS O, oNode AS ON, dgid AS D, dNode AS DN FROM mercer_ODpairs WHERE status = 1 LIMIT 1;
"""

#query to change status of selected OD pair from 1 to 2
Q_ChangeStatus2 = """
    UPDATE mercer_ODpairs SET status = 2 WHERE ogid = %s AND oNode = %s AND dgid = %s AND dNode = %s;
    COMMIT;
"""

#query to change status of selected OD pair from 2 to 3
Q_ChangeStatus3 = """
    UPDATE mercer_ODpairs SET status = 3 WHERE ogid = %s AND oNode = %s AND dgid = %s AND dNode = %s;
    COMMIT;
"""

TooLong = 0
NoPath = 0

runTimes = []
runTimesTooLong = []
runTimesNoPath = []

counter = 0


#test
#for i in xrange(0,10):
#    cur.execute(Q_SelectODpair)
#    Pair = cur.fetchall()
#    print Pair
#    oG = Pair[0][0]
#    oNo = Pair[0][1]
#    dG = Pair[0][2]
#    dNo = Pair[0][3]
#    cur.execute(Q_ChangeStatus2 % (oG, oNo, dG, dNo))
#    cur.execute(Q_SelectODpair)
#    Pair = cur.fetchall()
#    print Pair




while True:
    cur.execute(Q_SelectODpair)
    Pair = cur.fetchall()
    if len(Pair) == 0:
        break
    oG = Pair[0][0]
    oNo = Pair[0][1]
    dG = Pair[0][2]
    dNo = Pair[0][3]
    #change the status of the pair in the table to in progress (2)
    cur.execute(Q_ChangeStatus2 % (oG, oNo, dG, dNo))
    counter += 1
    for (oGID, oNode, dGID, dNode) in Pair:
        start_time = time.time()
        threshold = 10
        #calculate the shortest path
        cur.execute(Q_ShortestPath % (counter, oGID, dGID, oNode, dNode))
        results = cur.fetchall()
        if len(results) > 0:
            if results[-1][-1] <= threshold:
                #if there are results and the path is not too long, insert the results into the table
                cur.executemany(Q_InsertShortestPath, results)
                con.commit()
                #change the status of the pair in the table to calculated (3)
                cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
                runTimes.append(time.time() - start_time)
            else:
                #if the path it too long, still change the status, but don't add it to the table
                TooLong += 1
                cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
                runTimesTooLong.append(time.time() - start_time)
        else:
            #if there is no path, just change the status
            NoPath +=1
            cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
            runTimesNoPath.append(time.time() - start_time)



#OLD
#for i in xrange(0, 10):
#    start_time = time.time()
#    #grab an OD from the table
#    cur.execute(Q_SelectODpair)
#    Pair = cur.fetchall()
#    #identify its parts
#    oG = Pair[0][0]
#    oNo = Pair[0][1]
#    dG = Pair[0][2]
#    dNo = Pair[0][3]
#    #change the status of the pair in the table to in progress (2)
#    cur.execute(Q_ChangeStatus2 % (oG, oNo, dG, dNo))
#    counter += 1
#    for (oGID, oNode, dGID, dNode) in enumerate(Pair)
#        threshold = 10
#        #calculate the shortest path
#        cur.execute(Q_ShortestPath % (counter, oGID, dGID, oNode, dNode))
#        results = cur.fetchall()
#        if len(results) > 0:
#            if results[-1][-1] <= threshold:
#                #if there are results and the path is not too long, insert the results into the table
#                cur.executemany(Q_InsertShortestPath, results)
#                con.commit()
#                #change the status of the pair in the table to calculated (3)
#                cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
#                runTimes.append(time.time() - start_time)
#            else:
#                #if the path it too long, still change the status, but don't add it to the table
#                TooLong += 1
#                cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
#                runTimesTooLong.append(time.time() - start_time)
#        else:
#            #if there is no path, just change the status
#            NoPath +=1
#            cur.execute(Q_ChangeStatus3 % (oG, oNo, dG, dNo))
#            runTimesNoPath.append(time.time() - start_time)
            
print(TooLong)
print(NoPath)

avg = lambda iterable:sum(iterable)/float(len(iterable))
print min(runTimes), max(runTimes), avg(runTimes), sum(runTimes)


###ORIGINAL###
#count number of paths created that are too long to be added to table and number of paths that are not connected 
#create counter
#TooLong = 0
#NoPath = 0
#
#runTimes = []
#runTimesTooLong = []
#runTimesNoPath = []
#
#for i, (oGID, oNode, dGID, dNode) in enumerate(CloseEnough):
#    start_time = time.time()
#    threshold = 10
#    cur.execute(Q_ShortestPath % (i, oGID, dGID, oNode, dNode))
#    results = cur.fetchall()
#    if len(results) > 0:
#        if results[-1][-1] <= threshold:
#            cur.executemany(Q_InsertShortestPath, results)
#            con.commit()
#            runTimes.append(time.time() - start_time)
#        else:
#            TooLong += 1
#            runTimesTooLong.append(time.time() - start_time)
#    else:
#        NoPath +=1
#        runTimesNoPath.append(time.time() - start_time)
#
#
#print(TooLong)
#print(NoPath)
#
#avg = lambda iterable:sum(iterable)/float(len(iterable))
#print min(runTimes), max(runTimes), avg(runTimes), sum(runTimes)

