

VIEW = "links_l3_grp_%s" % str(sys.argv[1])


for i in xrange(1, 338):
    cur.execute("SELECT COUNT(*) FROM %s WHERE MIXID > 0" % (VIEW % i))
    cnt, = cur.fetchone()
    if cnt > 0:
        print VIEW % i, cnt
        with open("temp_processing.txt", "ab") as io:
            io.write("{0}: {1}\r\n".format(time.ctime(), i))
        p = subprocess.Popen([PYEXE, script, '%d' % i], stdout = subprocess.PIPE)
        p.communicate()