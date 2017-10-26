--CREATE TABLE IF NOT EXISTS tbl AS
SELECT
    _q.name,
    _q.county_code,
    tim23_area.name AS county_name,
    _q.cnt,
    _q.geojson
FROM (
    SELECT
        name,
        cnt,
        county_code,
        ST_AsGeoJSON(ST_Centroid(ST_Collect(multipoint)), 6) AS geojson
    FROM (
        SELECT
            LOWER(name) AS name,
            county_code,
            COUNT(*) AS cnt,
            array_agg(ST_Centroid(tim23_link.geom)) AS multipoint
        FROM link
        LEFT JOIN tim23_link
        ON link.fromnodeno = tim23_link.fromnodeno
        AND link.tonodeno = tim23_link.tonodeno
        GROUP BY name, county_code
        ORDER BY name
    ) AS _q0
) AS _q
LEFT JOIN tim23_area
ON _q.county_code = tim23_area.no
WHERE tim23_area.name IS NOT NULL
;