import VisumPy.helpers as h
import csv
import time

Visum = h.CreateVisum(15)
#load in bike stress version file


FromNode = h.GetMulti(Visum.Net.Links, "FromNodeNo",True)
ToNode = h.GetMulti(Visum.Net.Links, "ToNodeNo", True)
SpeedToUse = h.GetMulti(Visum.Net.Links, "SpeedToUse", True)

#create list of link identifiers, which is a concatenated string of FromNode+ToNode
LinkID = []

for i in xrange(len(FromNode)):
    LinkID.append(str(int(float(FromNode[i])))+str(int(float(ToNode[i]))))
    
#open and read list of links to change to PennDOT speed limits
with open(r'\\PEACH\Modeling\Projects\BikeStress\MORELinksToIncreaseModelSpeed.csv','rb') as IO:
    r = csv.reader(IO)
    header = r.next()
    LinkToChange = []
    for row in r:
        #row3 = nodecode, row5 = PennDOT speed limit
        insert = (row[3], int(row[5]))
        LinkToChange.append(insert)
        
#lookup index location of each node code in linkID list
#set speedtouse in that index position to PennDOT speed from CSV
for NodeCode, speed in LinkToChange:
  i = LinkID.index(NodeCode)
   SpeedToUse[i] = speed
 
#update SpeedToUse value in model
h.SetMulti(Visum.Net.Links, "SpeedToUse", SpeedToUse, True)
    

