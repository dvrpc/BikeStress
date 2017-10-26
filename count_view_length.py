

VIEW = "links_l3_grp_%s" % str(sys.argv[1])


for i in xrange(1, 338):
    VIEW = "links_l3_grp_%s" % str(sys.argv[1])
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (VIEW % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print (VIEW % i, "Count:", cnt)
        
        # with open("temp_processing.txt", "ab") as io:
            # io.write("{0}: {1}\r\n".format(time.ctime(), i))
        # p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        # p.communicate()
        
        
#SQL
#identify islands with only 2 links (the rest are turns)
WITH linkcount AS(
	SELECT strong, COUNT(*) as cnt_lnks  FROM master_links_grp
	WHERE mixid > 0
	GROUP BY strong)

SELECT * FROM linkcount
WHERE cnt_lnks = 2
ORDER BY strong;

#identify the links with geoms in those islands to view in QGIS
WITH linkcount AS(
	SELECT strong, COUNT(*) as cnt_lnks  FROM master_links_grp
	WHERE mixid > 0
	GROUP BY strong)

SELECT M.mixid, M.strong, M.geom, L.* FROM linkcount L
INNER JOIN master_links_grp M
ON M.strong = L.strong
WHERE L.cnt_lnks = 2
ORDER BY L.strong