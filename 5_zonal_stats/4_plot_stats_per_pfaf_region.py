import geopandas as gpd
import glob
import pandas as pd 
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

## --- Data locations
# Image save location
save_path = Path('/Users/wmk934/data/perceptual_models/img')

# Data location
agg_level = '10' # Aggregation level of the shapefiles, 1 to 10 (10 coarsest, 1 finest)
pfaf_path = Path('/Users/wmk934/data/Global_geospatial/MERIT_Hydro_basins/pfafstetter/aggregate/')
pfaf_files = glob.glob(str(pfaf_path) + f'/aggBasin_{agg_level}_G7*.shp')

zonal_path = Path('/Users/wmk934/data/perceptual_models/data/zonal_stats/')
pfaf_to_merit_file = 'agg_pfaf_to_merit_orig.shp'
csv_file = 'zonal_stats.csv'

## --- Data loading
# Load the shapefiles
pfafs = []
for file in pfaf_files:
    gdf = gpd.read_file(file)
    
    # We'll assign colors now, while we have the file handle available
    # This is easier than assigning them later based on the Pfaf codes,
    # since the Pfaf codes are not consistent across files:
    # - Most files have either 'L0C1xxxxx' 
    # - Endorheic basins have 'L0E0xxxxx'
    # - Pfaf 8 only has field 'L0C'
    # - Pfaf 4 only has field 'L0'
    # - Pfaf 2 only has field 'L0'
    # - Pfaf 1 has a null field
    cat = Path(file).stem[-1] # Should be 1-9 or E
    if cat == 'E':
        gdf['color'] = 'Endorheic' # Grey
    else:
        gdf['color'] = int(cat)
    pfafs.append(gdf)
pfaf = pd.concat(pfafs, ignore_index=True)
merit = gpd.read_file(zonal_path / pfaf_to_merit_file)

# Get the csv
stats = pd.read_csv(zonal_path / csv_file)

## --- Plotting prep
# Colors for each Pfaf region
colors = [
    (166, 206, 227), # Light blue
    ( 31, 120, 180), # Blue
    (178, 223, 138), # Light green
    ( 51, 160,  44), # Green
    (251, 154, 153), # Light red
    (227,  26,  28), # Red
    (253, 191, 111), # Light orange
    (255, 127,   0), # Orange
    (202, 178, 214), # Light purple
    (106,  61, 154), # Purple
    (190, 190, 190)  # Grey (no data)
]
colors_normalized = [(r / 255, g / 255, b / 255) for r, g, b in colors] # Normalize RGB values to range [0, 1]
cmap_full = ListedColormap(colors_normalized) # Create a colormap from the custom colors
cmap_pfaf = ListedColormap(colors_normalized[:10]) # Create a colormap from the custom colors, excluding grey

# FIG 1
# Check for which small Merit basins we have Pfaf codes
no_pfaf = (merit['pfaf'] == 'not assigned')

# Create a mock dataframe to get the correct color legend
cdf = gpd.GeoDataFrame({'color': [1, 2, 3, 4, 5, 6, 7, 8, 9, 'Endorheic', 'No Pfaf']},
                       geometry=gpd.points_from_xy([-100] * 11, [25] * 11))

# FIG 2:
# Assign color to the csv
stats['color'] = stats['pfaf'].apply(lambda x: x[3] if x.lower() != 'not assigned' else 'N')

### --- Plotting
# FIG 1: Pfafstetter regions
fig, ax = plt.subplots(1,1,figsize=(10, 10))
cdf.plot(ax=ax,  column='color', cmap=cmap_full, categorical=True, legend=True, legend_kwds={'loc': 'lower left', 'title': 'Pfafstetter region'})
pfaf.plot(ax=ax, column='color', cmap=cmap_pfaf, categorical=True, edgecolor='k', linewidth=0.5)
merit[no_pfaf].plot(ax=ax, color='grey', edgecolor='grey', linewidth=0.25) # merit second, plot only regions w/o Pfaf code
plt.tight_layout()
#plt.show()
plt.savefig(save_path / 'pfafstetter_regions.png', dpi=300, bbox_inches='tight')
plt.close()

# FIG 2: Zonal stats - individual boxplots, combinations of conditions come later

