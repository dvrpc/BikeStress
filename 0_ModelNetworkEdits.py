#network edits made in shapefile need to be incorporated into Visum
#assuming/hoping that FromNode/ToNode have remained consistent in newest version of model, this is an attempt to automate that process

import numpy
import VisumPy.helpers as h
import VisumPy.matrices as m
import csv
import os
import pandas as pd

Visum = h.CreateVisum(15)
#started with D:\BikePedTransit\BikeStress\versionfiles\Phase2_NJEdits\FromReubem_ModelDev_ScenMgr\2015_Base_Tim_2x_4_dec_2018 mk.ver

updatedlinks = 'U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\Updated_Links.csv'
circuitlinks = 'U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\Circuit_Links.csv'
alllinks = 'U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\Network_GISEdited_010219.csv'

#read in csv
updated = []
with open(updatedlinks, "rb") as io:
    r = csv.reader(io)
    header = r.next()
    for row in r: 
        updated.append(row)
        
#create blank lists to hold values
Num = []
FromNode = []
ToNode = []
NumLanes = []
BikeFac = []
Speed = []
Isoneway = []
TypeNo = []
UpdatedLinks = []

NullCounter = 0

#append values to lists and count null rows
for i in xrange(len(updated)):
    if updated[i][0] != '':
        Num.append(updated[i][0])
        FromNode.append(updated[i][1])
        ToNode.append(updated[i][2])
        NumLanes.append(updated[i][3])
        BikeFac.append(updated[i][4])
        Speed.append(updated[i][5])
        Isoneway.append(updated[i][6])
        TypeNo.append(updated[i][7])
        UpdatedLinks.append(updated[i][10])
    else:
        NullCounter += 1
        
#create new field to hold fromtonode combo
FromTo = []
for i in xrange(len(FromNode)):
    FromTo.append(str(FromNode[i])+str(ToNode[i]))
    
#collect values from model links
VNo               = h.GetMulti(Visum.Net.Links, r"No")
VFromNode         = h.GetMulti(Visum.Net.Links, r"FromNodeNo")
VToNode           = h.GetMulti(Visum.Net.Links, r"ToNodeNo")
VNumLanes         = h.GetMulti(Visum.Net.Links, r"NumLanes")
VBikeFac          = h.GetMulti(Visum.Net.Links, r"Bike_Facility")
VSpeed            = h.GetMulti(Visum.Net.Links, r"VCur_PrTSys(Car)")
VOneWay           = h.GetMulti(Visum.Net.Links, r"IsOneWayRoad")
VLinkType         = h.GetMulti(Visum.Net.Links, r"TypeNo")

#create new field to hold model fromtonode combo
VFromTo = []
for i in xrange(len(VFromNode)):
    VFromTo.append(str(int(VFromNode[i]))+str(int(VToNode[i])))
    
#find index locations of links that are in updated list
VUpdateLink = []
for i in xrange(len(VFromTo)):
    if VFromTo[i] in FromTo:
        VUpdateLink.append(1)
    else:
        VUpdateLink.append(0)
        
#flag with 'gis_update' uda
h.SetMulti(Visum.Net.Links, "gis_update", VUpdateLink)
#view in Visum to see if geographic distribution matches(yes it does)

#to edit speeds
#read in csv of entire edited GIS links
allgis = []
with open(alllinks, "rb") as io:
    r = csv.reader(io)
    header = r.next()
    for row in r: 
        allgis.append(row)
        
#create blank lists to hold values
ANum = []
AFromNode = []
AToNode = []
ANumLanes = []
ABikeFac = []
ASpeed = []
AIsoneway = []
ATypeNo = []
AUpdatedLinks = []

#append values to lists and count null rows
for i in xrange(len(allgis)):
    ANum.append(allgis[i][0])
    AFromNode.append(allgis[i][1])
    AToNode.append(allgis[i][2])
    ANumLanes.append(allgis[i][3])
    ABikeFac.append(allgis[i][4])
    ASpeed.append(allgis[i][5])
    AIsoneway.append(allgis[i][6])
    ATypeNo.append(allgis[i][7])
    AUpdatedLinks.append(allgis[i][8])
    
#create new field to hold model fromtonode combo
AFromTo = []
for i in xrange(len(AFromNode)):
    AFromTo.append(str(int(AFromNode[i]))+str(int(AToNode[i])))
    
#find index locations of links that are in all gis links list
InGIS = []
for i in xrange(len(VFromTo)):
    if VFromTo[i] in AFromTo:
        InGIS.append(1)
    else:
        InGIS.append(0)
        
SpeedToUse = []
for i in xrange(len(VFromTo)):
    #if the link has a GIS match
    if InGIS[i] == 1:
        #find the location of the match in the GIS FromTo combo list
        loc = AFromTo.index(VFromTo[i])
        #use that locaiton to find the GIS speed for that link
        SpeedToUse.append(ASpeed[loc])
    #if the link does not have a GIS match
    else:
        #use '999' as a filler for speed
        SpeedToUse.append(999)
        #len SpeedToUse should = len VFromTo when complete
        
#update uda
h.SetMulti(Visum.Net.Links, "speedtouse", SpeedToUse)
#view in Visum to see if geographic distribution matches(yes it does)

#gather vCur_PrTSys(Car)
VCarSpeed = h.GetMulti(Visum.Net.Links, r"VCur_PrTSys(Car)")
#convert to integer
for i in xrange(len(VCarSpeed)):
    VCarSpeed[i]= int(VCarSpeed[i])
    
#many speedtouse = 999 were trails and transit only links (Link Type = 1-9)
#where the folling conditions are true, set speedtouse = vCur_PrTSys(Car)

for i in xrange(len(VFromTo)):
    #find the location of each SpeedToUse = 999
    if SpeedToUse[i] == 999:
        if int(VLinkType[i]) >=1 & int(VLinkType[i]) <= 9:
            if int(VBikeFac[i]) <> 4:
                SpeedToUse[i] = VCarSpeed[i]    
                
#this takes care of 11871 links
#961 remaining
#34 links changed manually - bike facility = 4, but typeno <> 7

#update uda
h.SetMulti(Visum.Net.Links, "speedtouse", SpeedToUse)
#view in Visum to see if geographic distribution matches(yes it does)

#one more time
#now the only 999s that remain are TypeNo = 7 or 8
#set the rest to 0 to avoid throwing LTS assignment results
for i in xrange(len(VFromTo)):
    if SpeedToUse[i] == 999:
        SpeedToUse[i] = 0
        
#update uda
h.SetMulti(Visum.Net.Links, "speedtouse", SpeedToUse)
#view in Visum to see if geographic distribution matches(yes it does)

#Still need to fix previously identified wonky links with incorrectly high speeds
#revert to speeds from backup shapefile (9/18 from the BikeFacEditing GDB in U:\FY2017 folder)
with open('U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\WonkyPALinks_CorrectSpeeds.csv', 'rb') as f:
    reader = csv.reader(f)
    backupspeeds = map(tuple, reader)
    
SpeedCopy = h.GetMulti(Visum.Net.Links, r"SPEEDTOUSE")

toupdate=[]
updatelocation = []
pullfromindex = []
for i in xrange(len(VFromTo)):
    if VFromTo[i] in wfromto:
        toupdate.append(VFromTo[i])
        updatelocation.append(i)
        pullfromindex.append(wfromto.index(VFromTo[i]))
        
for i in xrange(len(toupdate)):
    #print toupdate[i]
    #print OldCombo[updatelocation[i]]
    SpeedCopy[updatelocation[i]] = newspeed[pullfromindex[i]]
    
h.SetMulti(Visum.Net.Links, "SPEEDTOUSE", SpeedCopy)
#saved as D:\BikePedTransit\BikeStress\versionfiles\Phase2_NJEdits\FromReubem_ModelDev_ScenMgr\2015_Base_Tim_2x_15Jan2019_sm.ver

#now look at entire list to compare number of lanes
NumLanesDif = []
NumLanesSame = []
gisNumLanes = []
for i in xrange(len(VFromTo)):
    #if the link has a GIS match
    if InGIS[i] == 1:
        #find the location of the match in the GIS FromTo combo list
        loc = AFromTo.index(VFromTo[i])
        if int(VNumLanes[i]) == int(ANumLanes[loc]):
            NumLanesSame.append(i)
            #use 999 as a filler
            gisNumLanes.append(999)
        else:
            NumLanesDif.append(i)
            #use gis num lanes
            gisNumLanes.append(ANumLanes[loc])
     #if the link does not have a GIS match
    else:
        #use 990 as a filler for not in gis
        gisNumLanes.append(990)
   
   
print len(NumLanesDif)
print len(NumLanesSame)
print len(NumLanesDif)+len(NumLanesSame)
print len(gisNumLanes)

#input as UDA to look at graphically and see if we want to make adjustments to the ~1000 links that have a different numlanes
h.SetMulti(Visum.Net.Links, "gis_numlanes", gisNumLanes)
#after looking closely at the links where gisnumlanes is different than what is in the model, it appears that the model is more accurate.
#the GIS numlanes is based on an older version file and the discrepancies are often in places where expansion projects were underway
#leavnig the model numlanes

#now look at entire list to compare isonewayroad
OneWayDif = []
OneWaySame = []
gisOneWay = []
for i in xrange(len(VFromTo)):
    #if the link has a GIS match
    if InGIS[i] == 1:
        #find the location of the match in the GIS FromTo combo list
        loc = AFromTo.index(VFromTo[i])
        if int(VOneWay[i]) == int(AIsoneway[loc]):
            OneWaySame.append(i)
            #use 999 as a filler for same
            gisOneWay.append(999)
        else:
            OneWayDif.append(i)
            #use gis one way
            gisOneWay.append(AIsoneway[loc])
     #if the link does not have a GIS match
    else:
        #use 990 as a filler for not in gis
        gisOneWay.append(990)
   
print len(OneWayDif)
print len(OneWaySame)
print len(OneWayDif)+len(OneWaySame)
print len(gisOneWay)

h.SetMulti(Visum.Net.Links, "gis_oneway", gisOneWay)

#due to the development of the pedestrian model, one way roads are handled differently in TIM 2.3+
#the IsOneWayRoad attribute is no longer used
#instead, tsys does not include car (just open to peds)
#requires a new way of thinking about how to calculate the total number of lanes
#created new formula UDA called 'TotNumLanes' by adding 'NumLanes' and 'ReverseLink\NumLanes'
#update this in LTS assignment script


#need to identify/edit bike facility changes/segmentation/connections

