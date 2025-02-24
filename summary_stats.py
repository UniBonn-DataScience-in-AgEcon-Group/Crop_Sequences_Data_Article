import os

import geopandas as gpd
import pandas as pd

gpd.options.io_engine = 'pyogrio'

start_year = 2019
end_year = 2024
hist_dir = 'hist'
results_file_name = 'Crop_Sequences_NRW_2019_2024.parquet'

# Import all historical plots
hist_years = range(start_year, end_year)
print(f'Importing historical plots: {hist_years}')
plots_data = {}
for year in hist_years:
  print(f'Importing {year}')
  plots_data[year] = gpd.read_parquet(
    os.path.join(hist_dir, f'{year}.parquet')
  )

# Import current year plots
print(f'Importing current year plots: {end_year}')
plots_data[end_year] = gpd.read_parquet(
  f'{end_year}.parquet'
)
# print number of rows in plots_data[end_year]
print(f"Number of rows in {end_year}: {len(plots_data[end_year])}")

# Import merged results file
results = pd.read_parquet(results_file_name)
# print number of rows in results
print(f"Number of rows in results: {len(results)}")

# Get the 10 most common crop CODEs by area for each year
final_df = pd.DataFrame()
for y in plots_data:
  df = plots_data[y].groupby("CODE", as_index=False)["AREA_HA"].sum()
  df = df.sort_values("AREA_HA", ascending=False).head(10)
  df["year"] = y
  if (y == 2024):
    print(df.head())
  final_df = pd.concat([final_df, df], ignore_index=True)

combined_df_res = pd.DataFrame()
for y in plots_data:
  df = results.groupby(f"CODE_{y}", as_index=False)["AREA_HA"].sum()
  df = df.sort_values("AREA_HA", ascending=False)
  # rename CODE_year to CODE
  df.rename(columns={f"CODE_{y}": "CODE"}, inplace=True)
  # rename AREA_HA to AREA_HA_Result
  df.rename(columns={"AREA_HA": "AREA_HA_Result"}, inplace=True)
  df["year"] = y
  if (y == 2024):
    print(df.head())
  # join with final_df on CODE
  combined_df_res = pd.concat([combined_df_res, df], ignore_index=True)
  
code_lookup = {
  459: "Permanent grassland",
  115: "Winter wheat",
  411: "Silage maize",
  131: "Winter barley",
  171: "Grain maize",
  156: "Winter triticale",
  603: "Sugar beets",
  311: "Winter rapeseed",
  602: "Potatoes",
  424: "Arable grass",
  121: "Winter rye",
}

final_df = final_df.merge(combined_df_res, on=["CODE", "year"], how="left")

# add a column that calculates the difference between the two area columns
final_df["AREA_SIMILARITY_PERC"] = final_df["AREA_HA_Result"] / final_df["AREA_HA"] * 100

# add a NAME column that maps the CODE to the crop name
final_df["NAME"] = final_df["CODE"].map(code_lookup)

# re-order columns -> year, CODE, NAME, AREA_HA, AREA_HA_Result, AREA_SIMILARITY_PERC
final_df = final_df[["year", "CODE", "NAME", "AREA_HA", "AREA_HA_Result", "AREA_SIMILARITY_PERC"]]


# Save as xlsx and csv
final_df.to_excel("Comparison.xlsx", index=False)
final_df.to_csv("Comparison.csv", index=False)