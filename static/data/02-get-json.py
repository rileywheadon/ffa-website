import pandas as pd
import json

df1 = pd.read_csv("stations.csv")
df2 = pd.read_csv("statistics.csv")

# De-duplicate statistics
df2 = (df2
    .dropna()
    .sort_values("MAX", ascending = False)  
    .drop_duplicates(subset=["STATION_NUMBER", "YEAR"], keep = "first")
)

# Filter out stations with less than 30 data points
df2 = df2.sort_values(["STATION_NUMBER", "YEAR"])
counts = df2["STATION_NUMBER"].value_counts()
keep = counts[counts >= 30].index
df2 = df2[df2["STATION_NUMBER"].isin(keep)]

# Filter stations in df1
stations = df2["STATION_NUMBER"].unique()
df1 = df1[df1["STATION_NUMBER"].isin(stations)]

# Save stations to JSON
df1.to_json("stations.json", orient = "records")

# Save statistics to JSON
data = df2.groupby("STATION_NUMBER").agg({
    "YEAR": list,
    "MAX": list
}).to_dict(orient = "index")

with open("statistics.json", "w", encoding="utf-8") as f:
    json.dump(data, f)
