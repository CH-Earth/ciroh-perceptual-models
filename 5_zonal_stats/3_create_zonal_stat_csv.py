'''Takes a list of shapefiles as input, and generates a single CSV as output'''

import geopandas as gpd
import glob
import pandas as pd
from pathlib import Path

# Define the input and output paths
agg_level = '1' # Aggregation level of the shapefiles, 1 to 10 (10 coarsest, 1 finest)
pfaf_path = Path('/Users/wmk934/data/Global_geospatial/MERIT_Hydro_basins/pfafstetter/aggregate/')
pfaf_files = glob.glob(str(pfaf_path) + f'/aggBasin_{agg_level}_G7*.shp') + glob.glob(str(pfaf_path) + f'/aggBasin_{agg_level}_G8*.shp') # 7: North America, 8: Greenland

zonal_dir = Path('/Users/wmk934/data/perceptual_models/data/zonal_stats/')
zonal_files = ['north_america_elev_slope_im_imr_fs.gpkg',
               'merit_hydro_basins_lgrip30_counts.shp',
               'merit_hydro_basins_MODIS_IGBP_counts.shp',
               'merit_hydro_basins_pelletier_soil_depth_mean_max_min_std.shp',
               'agg_pfaf_to_merit_orig.shp']
output_file = Path('/Users/wmk934/data/perceptual_models/data/zonal_stats/zonal_stats.csv')

# Read the pfaf shapefiles
pfafs = [gpd.read_file(file) for file in pfaf_files]
pfaf = pd.concat(pfafs, ignore_index=True)

# Read the zonal stat shapefiles
gdfs = [gpd.read_file(zonal_dir/file) for file in zonal_files]
for gdf in gdfs:
    gdf.set_index('COMID', inplace=True)

# Calculate the areas in our projection of choice
areas = gdfs[0][['unitarea','geometry']].copy()
areas = areas.to_crs('ESRI:102009')
areas['area_km2'] = areas.area / 10**6
areas.drop(columns=['unitarea'], inplace=True)
gdfs.append(areas)

# Remove the geometry column
for gdf in gdfs:
    gdf.drop(columns=['geometry'], inplace=True)

# Concatenate the dataframes
csv = pd.concat(gdfs, axis=1)
csv.to_csv(output_file)