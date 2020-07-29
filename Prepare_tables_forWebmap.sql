ALTER TABLE edgecounts_rail_counties
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying;
ALTER TABLE edgecounts_trolley_counties
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying;
ALTER TABLE edgecounts_bus_counties
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying;
ALTER TABLE edgecounts_trails_counties
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying;
ALTER TABLE edgecounts_full_counties
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying;
COMMIT;

UPDATE edgecounts_rail_counties
SET bikefacility = 'No Accomodation'
WHERE bike_fac_2 = 0;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Sharrows'
WHERE bike_fac_2 = 1;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Bike Lane'
WHERE bike_fac_2 = 2;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Buffered Bike Lane'
WHERE bike_fac_2 = 3;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Protected Bike Lane'
WHERE bike_fac_2 = 4;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Bike Route'
WHERE bike_fac_2 = 5;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Off-road Trail/Path'
WHERE bike_fac_2 = 6;
UPDATE edgecounts_rail_counties
SET bikefacility = 'Opposite Direction'
WHERE bike_fac_2 = 9;
COMMIT;
-- sub in other tables and repeat

UPDATE edgecounts_rail_counties
SET lts = 'LTS 1'
WHERE linklts >= 0.01 AND linklts <=0.10;
UPDATE edgecounts_rail_counties
SET lts = 'LTS 2'
WHERE linklts > 0.10 AND linklts <= 0.30;
UPDATE edgecounts_rail_counties
SET lts = 'LTS 3'
WHERE linklts > 0.30 AND linklts <= 0.60;
UPDATE edgecounts_rail_counties
SET lts = 'LTS 4'
WHERE linklts > 0.60;
COMMIT;
-- sub in other tables and repeat

--create rank column
ALTER TABLE edgecounts_rail_counties
ADD COLUMN rank integer;
COMMIT;
--update for each table name and repeat

UPDATE edgecounts_rail_counties
SET rank = w.rnk
FROM (
	SELECT 
		c.edge,
		ROW_NUMBER() OVER (ORDER BY c.total DESC) AS rnk
	FROM (
		SELECT *
		FROM edgecounts_rail_counties
		WHERE lts = 'LTS 3'
		AND co_name = 'Bucks'
		) c
	) w
WHERE edgecounts_rail_counties.edge = w.edge
;
--update for each county name and repeat
--update for each table name and repeat

--create percent column
Q_addcol = """
    ALTER TABLE "{0}"
    ADD COLUMN percent integer;
    COMMIT;
    """
for tab in tablelist:
    cur.execute(Q_addcol.format(tab))
--repeat for all tables

--use rank column to assign percentages
-- run in python below
Q_updatepercent = """

    UPDATE "{0}"
    SET percent = 10
    WHERE co_name = '{1}'
    AND rank <= (
        SELECT 
            COUNT(*)/10 AS top10
        FROM public."{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        );
        
    UPDATE "{0}"
    SET percent = 20
    WHERE co_name = '{1}'
    AND rank <= (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*2
    AND rank > (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        );
        
    UPDATE "{0}"
    SET percent = 30
    WHERE co_name = '{1}'
    AND rank <= (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*3
    AND rank > (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*2;
        
    UPDATE "{0}"
    SET percent = 40
    WHERE co_name = '{1}'
    AND rank <= (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*4
    AND rank > (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*3;
        
    UPDATE "{0}"
    SET percent = 50
    WHERE co_name = '{1}'
    AND rank <= (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*5
    AND rank > (
        SELECT 
            COUNT(*)/10 AS top10
        FROM "{0}"
        WHERE co_name = '{1}'
        AND lts = 'LTS 3'
        )*4;
    """
    
import psycopg2 as psql
con = psql.connect(dbname = "BikeStress_p2", host = "localhost", port = 5432, user = "postgres", password = "sergt")
cur = con.cursor()


TBL_rail =     "edgecounts_rail_counties"
TBL_trolley =  "edgecounts_trolley_counties"
TBL_bus =      "edgecounts_bus_counties"
TBL_trails =   "edgecounts_trails_counties"
TBL_full =     "edgecounts_full_counties"

countylist = ['Bucks', 'Burlington', 'Camden', 'Chester', 'Delaware', 'Gloucester', 'Mercer', 'Montgomery', 'Philadelphia']
tablelist = [TBL_rail, TBL_trolley, TBL_bus, TBL_trails, TBL_full]

for tab in tablelist:
    for county in countylist:
        print tab, county
        cur.execute(Q_updatepercent.format(tab, county))
        con.commit()
        
TBL_final_rail =     "lts3_rail_results"
TBL_final_trolley =  "lts3_trolley_results"
TBL_final_bus =      "lts3_bus_results"
TBL_final_trails =   "lts3_trails_results"
TBL_final_full =     "lts3_full_results"

finaltables = [TBL_final_rail, TBL_final_trolley, TBL_final_bus, TBL_final_trails, TBL_final_full]

for i in xrange(len(tablelist)):
    cur.execute("""
        CREATE TABLE "{1}" AS(
        SELECT 
            gid,
            edge AS link,
            ROUND(total, 0) AS total,
            lts AS linklts,
            totnumla_1 AS numlanes, 
            bikefacility,
            speedtouse AS speed,
            co_name AS county,
            percent,
            geom
        FROM "{0}"
        WHERE percent IS NOT NULL
        );""".format(tablelist[i], finaltables[i]))
    con.commit()

--doesnt have to be in python
ALTER TABLE lts3_rail_results
ADD COLUMN transitmode character varying;
ALTER TABLE lts3_trolley_results
ADD COLUMN transitmode character varying;
ALTER TABLE lts3_bus_results
ADD COLUMN transitmode character varying;
COMMIT;

UPDATE lts3_rail_results
SET transitmode = 'rail';
UPDATE lts3_trolley_results
SET transitmode = 'trolley';
UPDATE lts3_bus_results
SET transitmode = 'bus';
COMMIT;
--repeat for other transit tables



--create table to combine transit results into single layer with percent column for each transit mode

CREATE TABLE lts3_transit_results AS(
WITH tblA AS(
	SELECT
		link,
		linklts,
		numlanes,
		bikefacility,
		speed,
		county,
		geom
	FROM lts3_rail_results
	UNION
	SELECT
		link,
		linklts,
		numlanes,
		bikefacility,
		speed,
		county,
		geom
	FROM lts3_trolley_results
	UNION
	SELECT
		link,
		linklts,
		numlanes,
		bikefacility,
		speed,
		county,
		geom
	FROM lts3_bus_results
	)
SELECT
	a.link,
	a.linklts,
	a.numlanes,
	a.bikefacility,
	a.speed,
	a.county,
	r.total AS rail_total,
	t.total AS trolley_total,
	b.total as bus_total,
	r.percent AS rail_percent,
	t.percent AS trolley_percent,
	b.percent AS bus_percent,
	a.geom
FROM tblA a
LEFT JOIN lts3_rail_results r
ON a.link = r.link
LEFT JOIN lts3_trolley_results t
ON a.link = t.link
LEFT JOIN lts3_bus_results b
ON a.link = b.link);
COMMIT;

--combine all 3 transit modes to you know what to symbolize on
--only includes lts 3 roads that were in the top 10% each county for at least one of the 3 transit modes
ALTER TABLE lts3_transit_results
ADD COLUMN con_modes integer;
COMMIT;

-- 2 are null
UPDATE lts3_transit_results
SET con_modes = 1
WHERE trolley_percent IS NULL
AND bus_percent IS NULL;
UPDATE lts3_transit_results
SET con_modes = 1
WHERE rail_percent IS NULL
AND bus_percent IS NULL;
UPDATE lts3_transit_results
SET con_modes = 1
WHERE rail_percent IS NULL
AND trolley_percent IS NULL;
COMMIT;

--one is null
UPDATE lts3_transit_results
SET con_modes = 2
WHERE rail_percent IS NULL
AND trolley_percent IS NOT NULL
AND bus_percent IS NOT NULL;
UPDATE lts3_transit_results
SET con_modes = 2
WHERE trolley_percent IS NULL
AND rail_percent IS NOT NULL
AND bus_percent IS NOT NULL;
UPDATE lts3_transit_results
SET con_modes = 2
WHERE bus_percent IS NULL
AND rail_percent IS NOT NULL
AND trolley_percent IS NOT NULL;
COMMIT;

--none are null
UPDATE lts3_transit_results
SET con_modes = 3
WHERE rail_percent IS NOT NULL
AND trolley_percent IS NOT NULL
AND bus_percent IS NOT NULL;
COMMIT;


--run this for each table/mode
--be sure to index first (btree and gist)
CREATE TABLE "con_islands_full" AS(
WITH buf AS(
    SELECT link, st_buffer(geom, 10) buffer
    FROM "lts3_full_results"),

tblA AS(
    SELECT 
	DISTINCT(strong),
	goo.link
    FROM(
	SELECT 
	    L.mixid, 
	    L.strong, 
	    L.geom, 
	    B.link, 
	    B.buffer
	FROM "l2_master_links_grp" L
    INNER JOIN (
	SELECT 
	link,
	buffer
	FROM buf) B
    ON ST_Intersects(L.geom, B.buffer)) goo)

SELECT 
    tblA.link,
    string_agg(strong::text, ', ') AS islands,
    g.geom
FROM tblA
INNER JOIN "lts3_full_results" g
ON tblA.link = g.link
GROUP BY tblA.link, g.geom
);

--join to lts3_results tables to have in shapefile
CREATE TABLE lts3_rail_results2 AS(
	SELECT 
		l.*,
		c.islands AS con_islands
	FROM lts3_rail_results l
	LEFT JOIN con_islands_rail c
	ON l.link = c.link
	);
--repeat for each table