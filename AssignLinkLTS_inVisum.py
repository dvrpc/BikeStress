import numpy
import VisumPy.helpers as h
import VisumPy.matrices as m
import csv
import os



#create SpeedToUse UDA and set to VCur_PrTSyS(Car)

#grab Link attributes
FromNode         = h.GetMulti(Visum.Net.Links, r"FromNodeNo")
#ToNode           = h.GetMulti(Visum.Net.Links, r"ToNodeNo")
#Length           = h.GetMulti(Visum.Net.Links, r"Length")
NumLanes         = h.GetMulti(Visum.Net.Links, r"NumLanes")
BikeFac          = h.GetMulti(Visum.Net.Links, r"Bike_Facility")
Speed            = h.GetMulti(Visum.Net.Links, r"SpeedToUse")
OneWay           = h.GetMulti(Visum.Net.Links, r"IsOneWayRoad")
LinkType         = h.GetMulti(Visum.Net.Links, r"TypeNo")

TotLanes = [0] * len(FromNode)
#if one way, total number of lanes equals the number of lanes in the lane field
for i in xrange(0, len(FromNode)):
    if OneWay[i] == True:
        TotLanes[i] = NumLanes[i]
    #if not one way, total number of lanes equals 2 times the number of lanes in the lane field
    else:
        TotLanes[i] = NumLanes[i] * 2

#create lookup for lanes and speed columns in Figure 1 in paper
#second residential line modified to include speed up to 36 to make sure all fit into that category
road_index = [
    [(0, 0),   (0,  999)],
    [(-2, -2), (0,  25 )], 
    [(-2, -2), (26, 36 )], 
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

residential_index = [
    [(0, 0),   (0,  999)],
    [(1, 2),   (0,  25 )], 
    [(1, 2),   (26, 36 )], 
]


#create lookup for bike facility column in stress table
#numbers re-ordered to act as crosswalk between model bike fac codes and those in the reduction factor table from the paper
bikeFac_index = [0, 5, 1, 2, 3, 4, 9]

#from figure 1 in paper
#row 0 is filler for roads with no lanes
#column 7 is filler for roads with bike fac  =  (opposite direction of a one way street)
ReducFactorTbl = [
    [-1,   -1,   -1,   -1,  -1,   -1,   -1],
    [0.10, 0.10, 0.09, 0.05, 0.04, 0.03,-2],
    [0.15, 0.14, 0.14, 0.08, 0.05, 0.04,-2],
    [0.20, 0.19, 0.18, 0.10, 0.07, 0.05,-2],
    [0.35, 0.33, 0.32, 0.18, 0.12, 0.09,-2],
    [0.40, 0.38, 0.36, 0.20, 0.14, 0.10,-2],
    [0.67, 0.64, 0.60, 0.34, 0.23, 0.17,-2],
    [0.70, 0.67, 0.63, 0.35, 0.25, 0.18,-2],
    [0.80, 0.76, 0.72, 0.40, 0.28, 0.20,-2],
    [1.00, 0.95, 0.90, 0.50, 0.35, 0.25,-2],
    [1.20, 1.14, 1.08, 0.60, 0.42, 0.30,-2],
    [1.40, 1.33, 1.26, 0.70, 0.49, 0.35,-2],
]

#function to identify the row of Figure 1 in paper that the record falls in based on the total number of lanes and the speed
def findRowIndex(lanes, spd, link_type):
    if link_type in (72, 79) and lanes in (1, 2):
        for i, ((minlanes,maxlanes), (lowerspd, upperspd)) in enumerate(residential_index):
            if (minlanes <= lanes and lanes <= maxlanes) and (lowerspd <= spd and spd <= upperspd):
                return i
    else:
        for i, ((minlanes,maxlanes), (lowerspd, upperspd)) in enumerate(road_index):
            if (minlanes <= lanes and lanes <= maxlanes) and (lowerspd <= spd and spd <= upperspd):
                return i

#function to idenfity the colum of Figure 1 based on the bicycle facility
def bikeFacLookup(fac_code):
    for i, (facility) in enumerate(bikeFac_index):
        if facility == fac_code:
            return i
    

LinkStress = [0]* len(FromNode)            
for i in xrange(0, len(FromNode)):
	x = bikeFacLookup(BikeFac[i])
	y = findRowIndex(TotLanes[i], Speed[i], LinkType[i])
	LinkStress[i] = ReducFactorTbl[y][x]

#write into UDA field in Visum
h.SetMulti(Visum.Net.Links, "LinkLTS", LinkStress)

'''            
#write out to join to shapefile in ArcGIS (if needed)            
#create file and open for writing as the for loop iterates
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\output_data\LTS_output_test.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['FromNode', 'ToNode', 'NodeCode', 'LinkLTS', 'OneWay'])    
    #for each row (ID), use the lookup table to find the LTS value
    #also concatenate from node and to node to create nodecode field
    for i in xrange(0, len(FromNode)):
        x = bikeFacLookup(BikeFac[i])
        y = findRowIndex(TotLanes[i], Speed[i], LinkType[i])
        LinkStress = ReducFactorTbl[y][x]
        w.writerow([FromNode[i], ToNode[i], str(FromNode[i])+str(ToNode[i]), LinkStress[i], OneWay[i]])'''

        
#OLD
'''#read csv with link attributes
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\input_data\TIM_directed_links_TypeTest.csv','rb') as IO:
    r = csv.reader(IO)
    header = r.next() #read header row and move on to next before starting for loop
    #create array to hold data from csv
    Links = []
    for row in r: Links.append(([int(i) for i in row if i]))
        '''
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

