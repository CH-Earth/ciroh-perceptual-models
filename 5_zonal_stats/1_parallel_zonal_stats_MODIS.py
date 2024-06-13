'''
Function to run zonal_stats in parallel on multiple cores.
Based on: https://github.com/csc-training/geocomputing/blob/master/python/zonal_stats/zonal_stats_parallel.py

zonal_stats calculates statistics for each zone (=polygon) separately, so it is easy to parallelize
with deviding zones to different cores with multiprocessing map() function.

Usage:
    parallel_zonal_stats(raster, vector, zonal, n_cores, stats)
    raster: path to raster file
    vector: path to vector file
    zonal: path to output file
    n_cores: number of cores to use
    stats: list of statistics to calculate, e.g. ['mean', 'max', 'min', 'median', 'sum', 'std']
'''

from functools import partial
from multiprocessing import Pool
from rasterstats import zonal_stats
import geopandas
import rasterio
import time

# The task for one worker
def calculate_n(n, zones, data_array, affine, stats, nodata, categorical=False, category_map=None):
    if not categorical:
        return zonal_stats(zones.at[n,'geometry'], data_array, affine=affine, stats=stats, nodata=nodata)[0] # [0] because we're only getting one polygon
    else:
        return zonal_stats(zones.at[n,'geometry'], data_array, affine=affine, stats=stats, nodata=nodata, categorical=True, category_map=category_map)[0]

# main function
def parallel_zonal_stats(raster, vector, zonal, n_cores, stats, categorical=False, category_map=None):

    # Read data files
    try:
        zones = geopandas.read_file(vector)
    except Exception as e:
        print(f"Error reading vector file: {e}")
        return
    try:
        raster = rasterio.open(raster)
    except Exception as e:
        print(f"Error reading raster file: {e}")
        return

    # Get the data array and affine transformation matrix
    affine = raster.transform
    data_array = raster.read(1)
    nodata = raster.nodata

    # Create a sequence of numbers for each zone
    n = range(0, len(zones))
    
    # Create a pool of workers and run the function calculate_n for each zone
    # map() function creates in background batches
    with Pool(n_cores) as pool:
        partial_calculate_n = partial(calculate_n, zones=zones, data_array=data_array, affine=affine, stats=stats, nodata=nodata, categorical=categorical, category_map=category_map)
        results = pool.map(partial_calculate_n, n) 
    
    # Join the results back to geopandas dataframe
    for stat in stats:
        results_as_list = [d[stat] for d in results]
        zones[stat] = results_as_list  
        
    # Write the results to file
    try:
        zones.to_file(zonal)
    except Exception as e:
        print(f"Error writing output file: {e}")
        return
    
if __name__ == '__main__':

    start_time = time.time()

    # Define the paths to your files and other parameters
    raster = "/Users/wmk934/data/NorthAmerica_geospatial/modis_land/annual_mode_2001_2022/2001_2022_mode_MCD12Q1_LC_Type1.tif"
    vector = "/Users/wmk934/data/NorthAmerica_geospatial/merit_basins/MERIT_Hydro_modified_North_America_shapes/basins/cat_pfaf_7_8_MERIT_Hydro_v07_Basins_v01_bugfix1_hillslopes_pfaf_7_8_clean_fixed.shp"
    zonal = "/Users/wmk934/data/perceptual_models/data/zonal_stats/merit_hydro_basins_modis_igbp_counts.shp"
    n_cores = 4  # Number of cores to use
    stats = ['mean', 'max', 'min', 'std']  # Statistics to calculate
    categorical = True
    categories = {1: 'evergreen needleleaf forest',
                  2: 'evergreen broadleaf forest',
                  3: 'deciduous needleleaf forest',
                  4: 'deciduous broadleaf forest',
                  5: 'mixed forests',
                  6: 'closed shrublands',
                  7: 'open shrublands',
                  8: 'woody savannas',
                  9: 'savannas',
                  10: 'grasslands',
                  11: 'permanent wetlands',
                  12: 'croplands',
                  13: 'urban and built-up',
                  14: 'cropland/natural vegetation mosaic',
                  15: 'snow and ice',
                  16: 'barren or sparsely vegetated',
                  17: 'water bodies',
                  255:'unclassified'}
    
    # Run the function
    parallel_zonal_stats(raster, vector, zonal, n_cores, stats, categorical, categories)

    # At the end of your function, calculate and print the elapsed time
    elapsed_time = time.time() - start_time
    print(f"Elapsed time: {elapsed_time} seconds")