import sqlite3
import pandas as pd

conn = sqlite3.connect("Hydat.sqlite3")

# Get station information
query = """
SELECT 
    STATION_NUMBER,
    STATION_NAME,
    PROV_TERR_STATE_LOC AS LOCATION,
    LATITUDE AS LAT,
    LONGITUDE AS LON,
    DRAINAGE_AREA_GROSS,
    DRAINAGE_AREA_EFFECT,
    RHBN
FROM STATIONS
"""

df = pd.read_sql_query(query, conn)
df.to_csv("stations.csv", index=False)

# Get annual statistics 
query = """
SELECT STATION_NUMBER, YEAR, MAX
FROM ANNUAL_STATISTICS
"""

df = pd.read_sql_query(query, conn)
df.to_csv("statistics.csv", index=False)

# Close the connection
conn.close()
