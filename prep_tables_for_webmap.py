import psycopg2 as psql

#connect to SQL DB in python
con = psql.connect(dbname = "BikeStress_p3", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

priorities_tables = [
    'priorities_all',
    'priorities_all_ipd',
    'priorities_school',
    'priorities_school_ipd',
    'priorities_trail',
    'priorities_trail_ipd']

results_tables = [
    'results_all',
    'results_ipd_all',
    'results_rail',
    'results_ipd_rail',
    'results_school',
    'results_ipd_school',
    'results_trail',
    'results_ipd_trail',
    'results_trolley',
    'results_ipd_trolley']

#run after bus analysis completes
#transit tables =
    #['priorities_alltransit',
    #'priorities_alltransit_ipd',
    #'results_transit',
    #'results_ipd_transit'
    #]

Q_addcols = """
ALTER TABLE %s
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying,
ADD COLUMN id integer;
COMMIT;
"""

Q_update_bikefac = """
UPDATE %s
SET bikefacility = 'No Accomodation'
WHERE bikefac = 0;
UPDATE %s
SET bikefacility = 'Sharrows'
WHERE bikefac = 1;
UPDATE %s
SET bikefacility = 'Bike Lane'
WHERE bikefac = 2;
UPDATE %s
SET bikefacility = 'Buffered Bike Lane'
WHERE bikefac = 3;
UPDATE %s
SET bikefacility = 'Protected Bike Lane'
WHERE bikefac = 4;
UPDATE %s
SET bikefacility = 'Bike Route'
WHERE bikefac = 5;
UPDATE %s
SET bikefacility = 'Off-road Trail/Path'
WHERE bikefac = 6;
UPDATE %s
SET bikefacility = 'Opposite Direction'
WHERE bikefac = 9;
COMMIT;
"""

Q_update_lts = """
UPDATE %s
SET lts = 'LTS 1'
WHERE linklts >= 0.01 AND linklts <=0.10;
UPDATE %s
SET lts = 'LTS 2'
WHERE linklts > 0.10 AND linklts <= 0.30;
UPDATE %s
SET lts = 'LTS 3'
WHERE linklts > 0.30 AND linklts <= 0.60;
UPDATE %s
SET lts = 'LTS 4'
WHERE linklts > 0.60;
COMMIT;
"""

Q_update_id = """
UPDATE %s
SET id = edge;
COMMIT;
"""

def updatefields(table):
    cur.execute(Q_addcols % table)
    cur.execute(Q_update_bikefac % (table, table, table, table, table, table, table, table))
    cur.execute(Q_update_lts % (table, table, table, table))
    cur.execute(Q_update_id % table)

for table in priorities_tables:
    updatefields(table)

for table in results_tables:
    updatefields(table)
