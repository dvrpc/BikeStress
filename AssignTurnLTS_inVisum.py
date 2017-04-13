import numpy
import VisumPy.helpers as h
import VisumPy.matrices as m
import csv
import os

#in Visum

#grab turn attributes
FromNode         = h.GetMulti(Visum.Net.Turns, r"FromNodeNo")
ViaNode          = h.GetMulti(Visum.Net.Turns, r"ViaNodeNo")
ToNode           = h.GetMulti(Visum.Net.Turns, r"ToNodeNo")
#From_LinkType    = h.GetMulti(Visum.Net.Turns, r"FromLink\TypeNo")
#From_Lanes       = h.GetMulti(Visum.Net.Turns, r"FromLink\NumLanes")
#From_OneWay      = h.GetMulti(Visum.Net.Turns, r"FromLink\IsOneWayRoad")
#From_Spd         = h.GetMulti(Visum.Net.Turns, r"FromLink\VCur_PrTSys(Car)")
#From_BikeFac     = h.GetMulti(Visum.Net.Turns, r"FromLink\Bike_Facility")
To_LinkType      = h.GetMulti(Visum.Net.Turns, r"ToLink\TypeNo")
To_Lanes         = h.GetMulti(Visum.Net.Turns, r"ToLink\NumLanes")
To_OneWay        = h.GetMulti(Visum.Net.Turns, r"ToLink\IsOneWayRoad")
To_Spd           = h.GetMulti(Visum.Net.Turns, r"ToLink\VCur_PrTSys(Car)")
To_BikeFac       = h.GetMulti(Visum.Net.Turns, r"ToLink\Bike_Facility")
TurnDirection    = h.GetMulti(Visum.Net.Turns, r"TypeNo")
#NumEffLanes      = h.GetMulti(Visum.Net.Turns, r"NumEffLanes")
#grab max LTS value of all out links from Via Node
MaxApproachLTS   = h.GetMulti(Visum.Net.Turns, r"ViaNode\Max:OutLinks\LinkLTS")
#TurnLTS          = h.GetMulti(Visum.Net.Turns, r"TurnLTS")

#lanes based on total number of lanes - not number of lanes in each direction - so this needs to be addressed***
#create empty list to hold values

#From_TotLanes = [0] * len(FromNode)
##if one way, total number of lanes equals the number of lanes in the lane field
#for i in xrange(0, len(FromNode)):
#    if From_OneWay[i] == True:
#        From_TotLanes[i] = From_Lanes[i]
#    #if not one way, total number of lanes equals 2 times the number of lanes in the lane field
#    else:
#        From_TotLanes[i] = From_Lanes[i] * 2

To_TotLanes = [0] * len(FromNode)
for i in xrange(0, len(ToNode)):
    if To_OneWay[i] == True:
        To_TotLanes[i]   = To_Lanes[i]
    else:
        To_TotLanes[i]   = To_Lanes[i] * 2

#lookup and calculate LinkLTS value for from links and to links 

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
bikeFac_index = [0, 1, 2, 3, 4, 5, 9]

#from figure 1 in paper
#row 0 is filler for roads with no lanes
#column 7 is filler for roads with bike fac  = 9 (opposite direction of a one way street)
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

#function to #identify the row of Figure 1 in paper that the record falls in based on the total number of lanes and the speed
#first part evaluates residential roads; second part evaluates all other roads - second part should not return 0,1 or 2
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


#conditional to split how different types of turns are processed
#can indent back in to write out later
TurnStress = [0] * len(FromNode)
for i in xrange(0, len(FromNode)):
    #right turn - To Link Stress
    if TurnDirection[i] == 1:
        x = bikeFacLookup(To_BikeFac[i])
        y = findRowIndex(To_TotLanes[i], To_Spd[i], To_LinkType[i])
        TurnStress[i] = ReducFactorTbl[y][x]
    #left turn - ToLink Stress multiplied by 2
    elif TurnDirection[i] == 3:
        x = bikeFacLookup(To_BikeFac[i])
        y = findRowIndex(To_TotLanes[i], To_Spd[i], To_LinkType[i])
        TurnStress[i] = ReducFactorTbl[y][x] * 2           
    #straight - base LTS based on Max LTS of from/to links
    elif TurnDirection[i] == 2:
        TurnStress[i] = MaxApproachLTS[i]
    #plug in dummy value for u-turns - turn direction = 4
    else:
        TurnStress[i] = 99
    #print([FromNode[i], ViaNode[i], ToNode[i], TurnDirection[i], TurnStress[i]])
        
    #set turn attribute in Visum
    h.SetMulti(Visum.Net.Turns, "TurnLTS", TurnStress)

#to write out to CSV
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\output_data\TurnLTS_output.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['FromNode', 'ViaNode', 'ToNode', 'TurnDirection', 'TurnLTS'])        
    for i in xrange(0, len(FromNode)):
        w.writerow([FromNode[i], ViaNode[i], ToNode[i], TurnDirection[i], TurnStress[i]])
       
#to write to csv after TurnLTS is already assigned
with open(r'\\SMORAN\dvrpc_shared\PythonReference\BikeStressMap\output_data\TurnLTS_output.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['FromNode', 'ViaNode', 'ToNode', 'MaxApproachLTS', 'TurnDirection', 'TurnLTS'])        
    for i in xrange(0, len(FromNode)):
        w.writerow([FromNode[i], ViaNode[i], ToNode[i], MaxApproachLTS[i], TurnDirection[i], TurnLTS[i]])

