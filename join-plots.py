help = """
Uni Bonn - Crop rotation joiner

Joins crop type information from multiple years using spatial joins 
and largest area overlap to determine crop rotations on a single plot level.

Usage:
  python join-plots.py --cur <current_year_file> --hist <historical_files_folder> --out <result_file>

Example:
  python join-plots.py --cur ./test/input/2023.json --hist ./test/input --out ./test/output/joined-plots.shp
  python3.12 join-plots.py --cur ./hist/schlaege_2023.gpkg --hist ./hist --out ./joined-plots-2015-2023.parquet
"""

import argparse
import math
import os
import re

import geopandas as gpd
from tqdm import tqdm

gpd.options.io_engine = 'pyogrio'
tqdm.pandas()
parser=argparse.ArgumentParser(add_help=False)

parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help=help)
parser.add_argument("--cur", help="Path to the current year plot file")
parser.add_argument("--hist", help="Path to the historical plots folder, containing one file per year, e.g. 2017.json, 2018.json, etc.")
parser.add_argument("--out", help="Path to the output file")
parser.add_argument("--key-hist", help="Key that stores the crop type in the historical plot files, default: CODE")
parser.add_argument("--key-cur", help="Key that stores the crop type in the current plot file, default: CODE")
parser.add_argument("--id-hist", help="Key that stores the plot ID in the historical plot files, default: ID")
parser.add_argument("--id-cur", help="Key that stores the plot ID in the current plot files, default: ID")

args=parser.parse_args()

current_year_file = args.cur
historical_files_folder = args.hist
result_file = args.out
crop_key_hist = args.key_hist if args.key_hist else 'CODE'
crop_key_cur = args.key_cur if args.key_cur else 'CODE'
id_key_hist = args.id_hist if args.id_hist else 'ID'
id_key_cur = args.id_cur if args.id_cur else 'ID'

# Import current year plots
print(f'Importing current year plots: {current_year_file}')
if current_year_file.endswith('.parquet'):
  plots_current = gpd.read_parquet(current_year_file)
else:
  plots_current = gpd.read_file(current_year_file)
# plot
plots_current.set_crs(epsg=25832, inplace=True)
cur_year = re.search(r'\d{4}', current_year_file).group()
# rename crop_key column to crop_key_{cur_year}
plots_current[id_key_cur] = plots_current.index + 1
plots_current.rename(columns={crop_key_cur: f"CODE_{cur_year}"}, inplace=True)
# Print number of rows in plots_current
print(f"Number of rows in {cur_year}: {len(plots_current)}")


# Import historical plots (except current year file), 
# extract year from filename
# and rename {crop_key} column to {crop_key}_{year}
crop_types = {}
start_year = ""
for file in os.listdir(historical_files_folder):
  if not file.startswith('.'):
    year = int(re.search(r'\d{4}', file)[0])
    if not start_year or year < start_year:
      start_year = year
    # Don't import the current year file
    if year == int(cur_year):
      continue
    print(f'Importing {year}')
    if file.endswith('.parquet'):
      crop_types[year] = gpd.read_parquet(
        os.path.join(historical_files_folder, file), 
        columns=['geometry', crop_key_hist, id_key_hist]
      )
    else:
      crop_types[year] = gpd.read_file(
        os.path.join(historical_files_folder, file), 
        columns=['geometry', crop_key_hist, id_key_hist]
      )
    crop_types[year].set_crs(epsg=25832, inplace=True)
    # print number of rows in crop_types[year]
    print(f"Number of rows in {year}: {len(crop_types[year])}")
    crop_types[year][id_key_hist] = crop_types[year].index + 1
    crop_types[year].rename(columns={crop_key_hist: f"CODE_{year}"}, inplace=True)
    crop_types[year].rename(columns={id_key_hist: id_key_cur}, inplace=True)
    
# print keys in crop_types
print('Crop types:')
print(crop_types.keys())

def get_intersection_area(row):
  try:
    area = row['geometry'].intersection(row['geometry_right']).area
    return area
  except:
    return 0
    
def stringify_row(row):
  row = row[1]
  return f"{row[f'{id_key_cur}_right']}_{row['intersection']}_{row[f'{crop_key_cur}_{year}']}"

# Perform a spatial join, so that every plot from cur_year 
# has a {crop_key}_{year} property for the keys (years) in the crop_types dict
for year in crop_types.keys():
  print(f'Joining {year}')
  # Join all plots from {year} that intersect
  # with the current year plots
  crop_types[year]["geometry_right"] = crop_types[year].geometry
  plots_current = plots_current.sjoin(
    crop_types[year],
    how='left',
    predicate='intersects'
  )
  
  # Add a column with the area of the intersection
  plots_current["intersection"] = plots_current.progress_apply(
    # lambda row: row['geometry'].intersection(row['geometry_right']).area if row['geometry_right'] else 0, axis=1
    lambda row: get_intersection_area(row), axis=1
  )
  
  # remove rows where the intersection area is less than 1 square meter
  plots_current = plots_current[plots_current["intersection"] > 1]
  
  # Sort by intersection area and keep only the last row 
  # (largest intersection) for each plot
  plots_current = plots_current\
    .sort_values(by='intersection')\
    .groupby(f"{id_key_cur}_left")\
    .last()\
    .reset_index()
  
  # Drop the columns that are not needed anymore
  plots_current = plots_current.drop(
    columns=['index_right', 'intersection', 'geometry_right', f"{id_key_cur}_right"]
  )
  plots_current.set_crs(epsg=25832, inplace=True)
  # rename ID_left to ID
  plots_current.rename(columns={f"{id_key_cur}_left": id_key_cur}, inplace=True)
    # Print number of rows in plots_current
  print(f"Number of rows in {cur_year} after joining {year}: {len(plots_current)}")


# export the joined data
print('Exporting')
print(plots_current)
if not args.out:
  result_file = f'./joined-plots_{start_year}-{cur_year}.shp'

# check if result_file ends with .parquet
if result_file.endswith('.parquet'):
  plots_current.to_parquet(result_file)
else:
  plots_current.to_file(result_file)
# Also export as CSV, replace anything after the last dot with .csv
# without geometry column
plots_current.to_csv(
  re.sub(r'\.[^.]+$', '.csv', result_file), 
  columns=[c for c in plots_current.columns if c != 'geometry']
)