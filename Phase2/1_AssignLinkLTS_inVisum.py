import numpy
import VisumPy.helpers as h
import VisumPy.matrices as m
import csv
import os
import pandas as pd

Visum = h.CreateVisum(15)
# drag in version file
#D:\BikePedTransit\BikeStress\versionfiles\Phase2_NJEdits\Phase2_Network_010219.ver

#create SpeedToUse UDA and set to VCur_PrTSyS(Car)

# grab Link attributes
### make sure all these link attributes are populated, especially in newly added trail links
FromNode         = h.GetMulti(Visum.Net.Links, r"FromNodeNo")
ToNode           = h.GetMulti(Visum.Net.Links, r"ToNodeNo")
Length           = h.GetMulti(Visum.Net.Links, r"Length")
#NumLanes         = h.GetMulti(Visum.Net.Links, r"NumLanes")
TotLanes         = h.GetMulti(Visum.Net.Links, r"TotNumLanes")
BikeFac          = h.GetMulti(Visum.Net.Links, r"BIKE_FAC")
Speed            = h.GetMulti(Visum.Net.Links, r"SPEEDTOUSE")
OneWay           = h.GetMulti(Visum.Net.Links, r"ISONEWAY")#imported frin edited GIS file
LinkType         = h.GetMulti(Visum.Net.Links, r"TypeNo")


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
    [(1, 2),   (26, 65 )], 
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
    
'''
#for troubleshooting
##view from/to node nos to search for links that are causing errors so they can be fixed (only 12 in this round - mostly related to newly split links missing attributes)
x_error_rows = []
y_error_rows = []
for i in xrange(0, len(FromNode)):
    x = bikeFacLookup(BikeFac[i])
    y = findRowIndex(TotLanes[i], Speed[i], LinkType[i])
    if x is None:
        x_error_rows.append(i)
    if y is None:
        y_error_rows.append(i)
  
x_pairs=[]
for i in xrange(0, len(x_error_rows)):
    x_pairs.append(str(FromNode[x_error_rows[i]]) + ', ' +str(ToNode[x_error_rows[i]]))
    
y_pairs=[]
for i in xrange(0, len(y_error_rows)):
    y_pairs.append(str(FromNode[y_error_rows[i]]) + ', ' +str(ToNode[y_error_rows[i]]))
 

#write into UDA field in Visum
h.SetMulti(Visum.Net.Links, "LinkLTS", LinkStress)
#save version file
#Export directed links as shapefile
#Use PostGIS shapefile importer to import into DB


#additional troubleshooting
#to fix random large swath of links with incorrectly high speeds, revert to speeds from backup shapefile (9/18 from the BikeFacEditing GDB in U:\FY2017 folder)
import csv
with open('U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\WonkyPALinks_CorrectSpeeds.csv', 'rb') as f:
    reader = csv.reader(f)
    backupspeeds = map(tuple, reader)
    
wfromto = []
newspeed =[]
for i in xrange(len(backupspeeds)):
    fromto.append(backupspeeds[i][0])
    newspeed.append(backupspeeds[i][1])
    
OldFrom        = h.GetMulti(Visum.Net.Links, r"FromNode")
OldTo          = h.GetMulti(Visum.Net.Links, r"ToNode")
Speed          = h.GetMulti(Visum.Net.Links, r"SPEEDTOUSE")

SpeedCopy      = h.GetMulti(Visum.Net.Links, r"SPEEDTOUSE")

OldCombo = []
for i in xrange(len(OldFrom)):
    x = OldFrom[i]
    y = OldTo[i]
    if x is None and y is not None:
        OldCombo.append('NoneFrom'+str(int(y)))
    elif y is None and x is not None:
        OldCombo.append(str(int(x))+'NoneTo')
    elif x is None and y is None:
        OldCombo.append('None')
    else:        
        OldCombo.append(str(int(x))+str(int(y)))

#which indicies in OldCombo need to have speed updated in that same index

toupdate=[]
updatelocation = []
pullfromindex = []
for i in xrange(len(OldCombo)):
    if OldCombo[i] in fromto:
        toupdate.append(OldCombo[i])
        updatelocation.append(i)
        pullfromindex.append(fromto.index(OldCombo[i]))
        
        
for i in xrange(len(toupdate)):
    #print toupdate[i]
    #print OldCombo[updatelocation[i]]
    SpeedCopy[updatelocation[i]] = newspeed[pullfromindex[i]]
    
h.SetMulti(Visum.Net.Links, "SPEEDTOUSE", SpeedCopy)

'''