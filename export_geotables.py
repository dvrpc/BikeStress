"""
export_geotables.py
-------------------
This script illustrates the process of saving every
spatial table in a PostGIS database to shapefile.
Users may optionally specify the list of tables to
save manually. See "Step 3A" for directions.
"""

"""to run using python 3:
Open conda prompt
type 'activate py38'
type 'jupyter lab' to open jupyter interface
"""

import psycopg2
from pathlib import Path
from geopandas import GeoDataFrame

# Step 1: Change these parameters to point towards your database
# --------------------------------------------------------------
UN = "postgres"
PW = "password"
HOST = "localhost"
PORT = 5432
DB_NAME = "BikeStress_p3"

URI = f"postgresql://{UN}:{PW}@{HOST}:{PORT}/{DB_NAME}"

# Step 2: Define where you want to save the resulting shapefiles.
# ---------------------------------------------------------------
SHP_FOLDER = Path("//smoran01/dvrpc_shared/BikeLTS/Phase3/CleanShapefiles")


# Step 3A: Manually define a list of tables you want to export
#          To do so, update the "table_list" variable.
# ------------------------------------------------------------

#table_list_template = ["first_table", "second_table"]
table_list_template = ["priorities_all",
    "priorities_all_ipd",
    "priorities_school",
    "priorities_school_ipd",
    "priorities_trail",
    "priorities_trail_ipd"]

table_list = table_list_template


# Step 3B: Get a list of all spatial tables in the database.
#          Skip this if the user defined a custom "table_list"
# ------------------------------------------------------------

if table_list != table_list_template:

    sql_all_spatial_tables = """
        SELECT f_table_name AS tblname
        FROM geometry_columns
    """

    connection = psycopg2.connect(URI)
    cursor = connection.cursor()
    cursor.execute(sql_all_spatial_tables)

    spatial_tables = cursor.fetchall()
    table_list = [x[0] for x in spatial_tables]

    cursor.close()
    connection.close()


# Step 4: Iterate over the list of spatial tables, save each to shapefile
# -----------------------------------------------------------------------

print(f"Saving {len(table_list)} spatial tables to shapefile")

for table in table_list:

    print("\t-> Saving", table)

    query = f"SELECT * FROM {table}"

    connection = psycopg2.connect(URI)

    gdf = GeoDataFrame.from_postgis(query,
                                    connection,
                                    geom_col="geom")

    connection.close()

    shp_path = SHP_FOLDER / f"{table}.shp"
 
    gdf.to_file(shp_path)