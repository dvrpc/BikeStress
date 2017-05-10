import cPickle

#save a backup copy of the pickle
#read in a split pickle
with open(...) as io:
    data = cPickle.load(io)

len(data)
batch_size = 1000000

counter = 0
i = 0

for j in xrange(batch_size, len(data), batch_size):
    with open(r"D:\Modeling\BikeStress\group_180_%02d.cpickle" % counter, "wb") as io:
        cPickle.dump(data[i:j], io)

    i = j
    counter += 1
with open(r"D:\Modeling\BikeStress\group_180_%02d.cpickle" % counter, "wb") as io:
    cPickle.dump(data[i:len(data)], io)
    
    
    
#read in and process pickle slices
import psycopg2 as psql
import cPickle

TBL_ALL_LINKS = "montco_lts_links"
TBL_CENTS = "montco_blockcent"
TBL_LINKS = "montco__L3_tolerablelinks"
TBL_NODES = "montco_nodes"
TBL_SPATHS = "montco_L3_shortestpaths_180"
TBL_TOLNODES = "montco__L3_tol_nodes"
TBL_GEOFF_LOOKUP = "montco_L3_geoffs"
TBL_GEOFF_GEOM = "montco_L3_geoffs_viageom"
TBL_MASTERLINKS = "montco_L3_master_links"
TBL_MASTERLINKS_GEO = "montco_L3_master_links_geo"
TBL_MASTERLINKS_GROUPS = "montco_L3_master_links_grp"
TBL_GROUPS = "montco_L3_groups"
TBL_OD = "montco_L3_OandD"
TBL_NODENOS = "montco_L3_nodenos"
TBL_NODES_GEOFF = "montco_L3_nodes_geoff"
TBL_NODES_GID = "montco_L3_nodes_gid"
TBL_GEOFF_NODES = "montco_L3_geoff_nodes"

VIEW = "links_l3_grp_180"

IDX_nx_SPATHS_value = "montco_spaths_nx_value_idx"


    
con = psql.connect(database = "BikeStress", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()

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

Q_SelectMasterLinks = """
    SELECT
        mixid,
        fromgeoff,
        togeoff,
        cost
    FROM public."{0}";
    """.format(VIEW)

cur.execute(Q_SelectMasterLinks)
MasterLinks = cur.fetchall()
•
node_pairs = {}       
for i, (mixid, fromgeoff, togeoff, cost) in enumerate(MasterLinks):
    node_pairs[(fromgeoff, togeoff)] = mixid

del MasterLinks
    
#run for each pickle slice    
with open(r"D:\Modeling\BikeStress\group_180_20.cpickle", "rb") as io:
    data = cPickle.load(io)

edges = []
for id, path in enumerate(data):
    oGID = nodes_gids[geoff_nodes[path[0]]]
    dGID = nodes_gids[geoff_nodes[path[-1]]]
    for seq, (o ,d) in enumerate(zip(path, path[1:])):
        row = id, seq, oGID, dGID, node_pairs[(o,d)]
        edges.append(row)
        
if (len(edges) > 0):
    Q_CreateOutputTable = """
        CREATE TABLE IF NOT EXISTS public."{0}"
        (
            id integer,
            seq integer,
            ogid integer,
            dgid integer,
            edge bigint
        )
        WITH (
            OIDS = FALSE
        )
        TABLESPACE pg_default;

        
        CREATE INDEX IF NOT EXISTS "{1}"
            ON public."{0}" USING btree
            (id, seq, ogid, dgid, edge)
            TABLESPACE pg_default;    
        COMMIT;
    """.format(TBL_SPATHS, IDX_nx_SPATHS_value)
    cur.execute(Q_CreateOutputTable)

    str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(edges[0]))))
    cur.execute("""BEGIN TRANSACTION;""")
    batch_size = 10000
    for i in xrange(0, len(edges), batch_size):
        j = i + batch_size
        arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in edges[i:j])
        #print arg_str
        Q_Insert = """INSERT INTO public."{0}" VALUES {1}""".format(TBL_SPATHS, arg_str)
        cur.execute(Q_Insert)
    cur.execute("COMMIT;")
    
del data, edges
print "done 20"



"""
-- CREATE TABLE public."montco_L3_shortestpaths" AS 
-- (SELECT * FROM public."montco_L3_shortestpaths_mario"
-- UNION ALL
-- SELECT * FROM public."montco_L3_shortestpaths_180");
"""


