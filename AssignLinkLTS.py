import numpy
import csv
import os
import pandas

#read csv with link attributes
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\input_data\TIM_directed_links_TypeTest.csv','rb') as IO:
    r = csv.reader(IO)
    header = r.next() #read header row and move on to next before starting for loop
    #create array to hold data from csv
    Links = []
    for row in r: Links.append(([int(i) for i in row if i]))

#create lookup for lanes and speed columns in Figure 1 in paper
#negatives are residential - pre-processed based on type number (72 and 79) and number of lanes (1 and 2)
#second residential line modified to include speed up to 36 to make sure all fit into that category
road_index = [
    [(0, 0),   (0,  999)],
    [(-2, -2),   (0,  25 )], 
    [(-2, -2),   (26, 36 )], 
    [(1, 3),   (0,  25 )],
    [(4, 5),   (0,  25 )],
    [(1, 3),   (26, 34 )],
    [(6, 999), (0,  25 )],
    [(4, 5),   (26, 34 )],
    [(6, 999), (26, 34 )],
    [(1, 3),   (35, 999)],
    [(4, 5),   (35, 999)],
    [(6, 999), (35, 999)],
]

#create lookup for bike facility column in stress table
bikeFac_index = [0, 1, 2, 3, 4, 5, 9]

#from figure 1 in paper
#row 0 is filler for roads with no lanes
#column 7 is filler for roads with bike fac  = 9 (opposite direction of a one way street)
ReducFactorTbl = [
    [8,    8,    8,    8,    8,    8,    8],
    [0.10, 0.10, 0.09, 0.05, 0.04, 0.03, 9],
    [0.15, 0.14, 0.14, 0.08, 0.05, 0.04, 9],
    [0.20, 0.19, 0.18, 0.10, 0.07, 0.05, 9],
    [0.35, 0.33, 0.32, 0.18, 0.12, 0.09, 9],
    [0.40, 0.38, 0.36, 0.20, 0.14, 0.10, 9],
    [0.67, 0.64, 0.60, 0.34, 0.23, 0.17, 9],
    [0.70, 0.67, 0.63, 0.35, 0.25, 0.18, 9],
    [0.80, 0.76, 0.72, 0.40, 0.28, 0.20, 9],
    [1.00, 0.95, 0.90, 0.50, 0.35, 0.25, 9],
    [1.20, 1.14, 1.08, 0.60, 0.42, 0.30, 9],
    [1.40, 1.33, 1.26, 0.70, 0.49, 0.35, 9],
]

#function to #identify the row of Figure 1 in paper that the record falls in based on the total number of lanes and the speed
def findRowIndex(lanes, spd):
    for i, ((minlanes,maxlanes), (lowerspd, upperspd)) in enumerate(road_index):
        if (minlanes <= lanes and lanes <= maxlanes) and (lowerspd <= spd and spd <= upperspd):
            return i

#function to idenfity the colum of Figure 1 based on the bicycle facility
def bikeFacLookup(fac_code):
    for i, (facility) in enumerate(bikeFac_index):
        if facility == fac_code:
            return i
            

#create file and open for writing as the for loop iterates
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\output_data\LTS_output_test.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['ID', 'NodeCode', 'LinkLTS', 'OneWay'])    
    #for each row (ID), use the lookup table to find the LTS value
    #also concatenate from node and to node to create nodecode field
    for row in Links:
        x = bikeFacLookup(row[6])
        y = findRowIndex(row[10], row[7])
        #write results
        #print([row[0], str(row[2])+str(row[3]), ReducFactorTbl[y][x], row[9]])
        w.writerow([row[0], str(row[2])+str(row[3]), ReducFactorTbl[y][x], row[9]])

        
#FOR TESTING
'''
#create place to put index results
IDs = []
values = []

#loop over all rows in the table and send them through the functions to identify the lookup location in the stress table
for row in Links[0:9]:
    IDs.append(row[0])
    x = bikeFacLookup(row[6])
    y = findRowIndex(row[10], row[7])
    values.append(stressTbl[y][x])
    
#maybe write out to CSV instead...    
holder = zip(IDs, xvals, yvals)
holder = zip(IDs, values)


if y in (1):
            z = BaseLTS[0]
        elif y in (2,3):
            z = BaseLTS[1]
        elif y in (4, 5):
            z = BaseLTS[2]
        elif y in (6, 7, 8, 9, 10, 11):
            z = BaseLTS[3]

'''