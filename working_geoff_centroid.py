Q_2LinkIslands = """
    WITH linkcount AS(
        SELECT strong, COUNT(*) as cnt_lnks  FROM {0}
        WHERE mixid > 0
        GROUP BY strong)

    SELECT strong FROM linkcount
    WHERE cnt_lnks = 2
    ORDER BY strong;
    """.format(TBL_MASTERLINKS_GROUPS)
cur.execute(Q_2LinkIslands)
TwoLinkIslands = cur.fetchall()

skip = []
for i in xrange(0,len(TwoLinkIslands)):
    skip.append(TwoLinkIslands[i][0])
    
    
    

import psycopg2 as psql
import csv
import itertools
import numpy
import time
import sys
import json
import scipy.spatial
import networkx as nx

TBL_ALL_LINKS = "sa_lts_links"
TBL_CENTS = "pa_blockcentroids"
TBL_LINKS = "sa_L3_tolerablelinks"
TBL_NODES = "sa_nodes"
# TBL_SPATHS = "montco_L3_shortestpaths_196"
TBL_TOLNODES = "sa_L3_tol_nodes"
TBL_GEOFF_LOOKUP = "geoffs"
TBL_GEOFF_GEOM = "geoffs_viageom"
TBL_MASTERLINKS = "master_links"
TBL_MASTERLINKS_GEO = "master_links_geo"
TBL_MASTERLINKS_GROUPS = "master_links_grp"

TBL_GROUPS = "groups"
TBL_NODENOS = "nodenos"
TBL_NODES_GEOFF = "nodes_geoff"
TBL_NODES_GID = "nodes_gid"
TBL_GEOFF_NODES = "geoff_nodes"
TBL_OD = "OandD"

con = psql.connect(dbname = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

#within datasetup? or network island analysis?
Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODENOS)
cur.execute(Q_GetList)
nodenos = cur.fetchall()

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODES_GEOFF)
cur.execute(Q_GetList)
nodes_geoff_list = cur.fetchall()
nodes_geoff = dict(nodes_geoff_list)

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_NODES_GID)
cur.execute(Q_GetList)
nodes_gids_list = cur.fetchall()
nodes_gids = dict(nodes_gids_list)

Q_GetList = """
    SELECT * FROM "{0}";
    """.format(TBL_GEOFF_NODES)
cur.execute(Q_GetList)
geoff_nodes_list = cur.fetchall()
geoff_nodes = dict(geoff_nodes_list)

#call OD list from postgres
Q_GetOD = """
    SELECT * FROM "{0}";
    """.format(TBL_OD)
cur.execute(Q_GetOD)
OandD = cur.fetchall()

Q_GeoffGroup = """
WITH geoff_group AS (
    SELECT
        fromgeoff AS geoff,
        strong
    FROM "{0}"
    WHERE strong IS NOT NULL
    GROUP BY fromgeoff, strong
    
    UNION ALL

    SELECT
        togeoff AS geoff,
        strong
    FROM "{0}"
    WHERE strong IS NOT NULL
    GROUP BY togeoff, strong
)
SELECT geoff, strong FROM geoff_group
GROUP BY geoff, strong
ORDER BY geoff DESC;
""".format(TBL_MASTERLINKS_GROUPS)

cur.execute(Q_GeoffGroup)
geoff_grp_list = cur.fetchall()
geoff_grp = dict(geoff_grp_list)

CloseEnough = []
DiffGroup = 0
NullGroup = 0
#are the OD geoffs in the same group? if so, add pair to list to be calculated
for i, (fromnodeindex, tonodeindex) in enumerate(OandD):
    #if i % pool_size == (worker_number - 1):
    fromnodeno = nodenos[fromnodeindex][0]
    tonodeno = nodenos[tonodeindex][0]
    if nodes_geoff[fromnodeno] in geoff_grp and nodes_geoff[tonodeno] in geoff_grp:
        if geoff_grp[nodes_geoff[fromnodeno]] == geoff_grp[nodes_geoff[tonodeno]]:
            # if geoff_grp[nodes_geoff[fromnodeno]] == int(sys.argv[1]):
                CloseEnough.append([
                    nodes_gids[fromnodeno],    # FromGID
                    fromnodeno,                # FromNode
                    nodes_geoff[fromnodeno],   # FromGeoff
                    nodes_gids[tonodeno],      # ToGID
                    tonodeno,                  # ToNode
                    nodes_geoff[tonodeno],     # ToGeoff
                    geoff_grp[nodes_geoff[fromnodeno]]  # GroupNumber
                    ])
        else:
            DiffGroup += 1
    else:
        NullGroup += 1
            
            
TBL_BLOCK_NODE_GEOFF = "block_node_geoff"
IDX_BLOCK_NODE_GEOFF = "block_node_geoff_value_idx"
TBL_GEOFF_GROUP = "geoff_group"
IDX_GEOFF_GROUP = "geoff_group_value_idx"

#write out close enough to table
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(

    fromgid     integer,
    fromnode    integer,
    fromgeoff   integer,
    togid       integer,
    tonode      integer,
    togeoff     integer,
    groupnumber integer,
    rowno       BIGSERIAL PRIMARY KEY
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (fromgid, fromnode, fromgeoff, togid, tonode, togeoff, groupnumber)
    TABLESPACE pg_default;    
""".format(TBL_BLOCK_NODE_GEOFF, IDX_BLOCK_NODE_GEOFF)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(CloseEnough[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(CloseEnough), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in CloseEnough[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_BLOCK_NODE_GEOFF, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")

Q_Index = """
    CREATE INDEX block_node_geoff_grp_idx
      ON public.{0}
      USING btree
      (groupnumber);

    COMMIT;
    """.format(TBL_BLOCK_NODE_GEOFF)
cur.execute(Q_Index)


#write out geoff_group to table
Q_CreateOutputTable = """
CREATE TABLE IF NOT EXISTS public."{0}"
(

    geoff integer,
    grp integer,
    rowno BIGSERIAL PRIMARY KEY
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;

CREATE INDEX IF NOT EXISTS "{1}"
    ON public."{0}" USING btree
    (geoff, grp)
    TABLESPACE pg_default;    
""".format(TBL_GEOFF_GROUP, IDX_GEOFF_GROUP)
cur.execute(Q_CreateOutputTable)

str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(geoff_grp_list[0]))))
cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(geoff_grp_list), batch_size):
    j = i + batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in geoff_grp_list[i:j])
    #print arg_str
    Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_GEOFF_GROUP, arg_str)
    cur.execute(Q_Insert)
cur.execute("COMMIT;")
            
            
            
            
            
#within shortest paths child
Q_GetGroupPairs = """
    SELECT
        fromgeoff AS fgeoff,
        togeoff AS tgeoff,
        groupnumber AS grp
    FROM "{0}"
    WHERE groupnumber = {1};
    """.format(TBL_BLOCK_NODE_GEOFF, int(sys.argv[1]))
cur.execute(Q_GetGroupPairs)
group_pairs = cur.fetchall()
    
pairs = []
for i, (fgeoff, tgeoff, grp) in enumerate(group_pairs):
    source = fgeoff
    target = tgeoff
    pairs.append((source, target))
    

#within shortest paths parent
Q_GeoffCount = """
SELECT 
    COUNT(*) AS cnt 
FROM (
    SELECT 
        fromgid, 
        togid 
    FROM "{0}" 
    WHERE groupnumber = %d AND fromgid <> togid 
    GROUP BY fromgid, togid )
    AS _q0
    """.format(TBL_BLOCK_NODE_GEOFF)

for i in xrange(1055, 5450):
cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (TBL_TEMP_NETWORK % i))
cnt, = cur.fetchone()
if cnt > 0:
    cur.execute(Q_GeoffCount, % i)
    geoffCount = cur.fetchall()
    if geoffCount > 0:
        cur.execute("SELECT ")
        print TBL_TEMP_NETWORK % i
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()
    


island = []
counts = []
for i in xrange(1, 5450):
    cur.execute(Q_GeoffCount % i)
    count = cur.fetchall()
    island.append(i)
    counts.append(count[0][0])

combo = [list(a) for a in zip(island, counts)]
with open(r"D:\Modeling\BikeStress\scripts\combo_counts.csv", "wb") as io:
    w = csv.writer(io)
    w.writerows(combo)
    
    