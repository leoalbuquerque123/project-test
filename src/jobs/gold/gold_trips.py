# fact_df = trips_df.join(
#     dim_zone_df.filter("is_current = true"),
#     trips_df.PULocationID == dim_zone_df.LocationID,
#     "left"
# )