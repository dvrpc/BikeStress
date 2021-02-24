ALTER TABLE priorities_all
ADD COLUMN lts character varying,
ADD COLUMN bikefacility character varying,
ADD COLUMN id integer;
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

UPDATE tbl
SET id = edge
COMMIT;
-- sub in other tables and repeat

--once everything is updated, run export_geotables.py to export to shapefiles
--drop old columns from shapefiles in qgis

