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
To_LinkLTS       = h.GetMulti(Visum.Net.Turns, r"ToLink\LINKLTS")
#To_LinkType      = h.GetMulti(Visum.Net.Turns, r"ToLink\TypeNo")
#To_Lanes         = h.GetMulti(Visum.Net.Turns, r"ToLink\NumLanes")
#To_OneWay        = h.GetMulti(Visum.Net.Turns, r"ToLink\ISONEWAY")
#To_Spd           = h.GetMulti(Visum.Net.Turns, r"ToLink\SPEEDTOUSE")
#To_BikeFac       = h.GetMulti(Visum.Net.Turns, r"ToLink\BIKE_FAC")
TurnDirection    = h.GetMulti(Visum.Net.Turns, r"TypeNo")
#grab max LTS value of all out links from Via Node
MaxApproachLTS   = h.GetMulti(Visum.Net.Turns, r"ViaNode\Max:OutLinks\LINKLTS")


#conditional to split how different types of turns are processed
TurnStress = [0] * len(FromNode)

for i in xrange(0, len(FromNode)):
    #right turn - To Link Stress
    if TurnDirection[i] == 1:
        TurnStress[i] = To_LinkLTS[i]
    #left turn - ToLink Stress multiplied by 2
    elif TurnDirection[i] == 3:
        TurnStress[i] = To_LinkLTS[i] * 2           
    #straight - base LTS based on Max LTS of from/to links
    elif TurnDirection[i] == 2:
        TurnStress[i] = MaxApproachLTS[i]
    #plug in dummy value for u-turns - turn direction = 4
    else:
        TurnStress[i] = 99

        
#create TurnLTS UDA in visum
#set turn attribute in Visum
h.SetMulti(Visum.Net.Turns, "TurnLTS", TurnStress)

       
#to write to csv after TurnLTS is already assigned
with open(r'U:\FY2019\Transportation\TransitBikePed\BikeStressPhase2\data\IntermediateOutputs\TurnLTS_output_010419.csv','wb') as IO:
    w = csv.writer(IO)
    #write the header row
    w.writerow(['FromNode', 'ViaNode', 'ToNode', 'MaxApproachLTS', 'TurnDirection', 'TurnLTS'])        
    for i in xrange(0, len(FromNode)):
        w.writerow([FromNode[i], ViaNode[i], ToNode[i], MaxApproachLTS[i], TurnDirection[i], TurnStress[i]])

