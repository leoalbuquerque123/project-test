import pandas as pd
from pathlib import Path

"""
Loading Zone Lookup file (CSV) to a Pandas Dataframe and checking its shape and head.
"""
df_zone = pd.read_csv("/mnt/c/Users/leona/OneDrive/Documents/Projeto Test/src/ingestion/taxi_zone_lookup.csv", sep=",")
# print(df_zone.shape)
# print(df_zone.head())


"""
Loading all the Taxi Trip files (Parquet) to a unique Pandas Dataframe and checking its shape and head.
"""
folder = Path("/mnt/c/Users/leona/OneDrive/Documents/Projeto Test/src/ingestion/data/")

files = folder.glob("yellow_tripdata_*.parquet")

df_trips = pd.concat(
    (pd.read_parquet(file) for file in files),
    ignore_index=True,
)
# print(df_trips.shape)
# print(df_trips.head())


"""
Merging both dataframes to check if there are any PULocationID that do not have a corresponding LocationID in the zone lookup.
"""
df_merged = pd.merge(df_trips, df_zone, left_on="PULocationID", right_on="LocationID", how="left")
df_final_filtered = df_merged[df_merged["LocationID"].isnull()]
# print(df_final_filtered.shape)
# print(df_final_filtered["PULocationID"].unique())

"""
Analyzing the Total Fare Amount by Month (Year-Month) for the merged dataframe.
"""
df_merged["year_month"] = df_merged["tpep_pickup_datetime"].dt.to_period("M")

df_agg = (
    df_merged.groupby("year_month")
        .agg(
            Total_Fare_Amount=("fare_amount", "sum"),
            Total_Trip_Distance=("trip_distance", "sum"),
            Count_Records=("fare_amount", "count"))
        .reset_index()
        .sort_values("year_month", ascending=False)
)
print(df_agg)
print("\n")

"""
Analyzing the Total Fare Amount by Borough for the merged dataframe.
"""
df_agg = (
    df_merged.groupby("Borough", dropna=False)["fare_amount"]
        .sum()
        .reset_index()
        .rename(columns={"fare_amount": "Total_Fare_Amount"})
)

pd.set_option("display.float_format", "{:,.2f}".format)
print(df_agg)