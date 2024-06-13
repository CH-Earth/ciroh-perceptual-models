# Resample
We have:
- Existing GeoTIFF files for North American domain:
    - Climate (WorldClim)
    - Forest height (GLCLUC)
    - LAI (MODIS)
    - Agriculture (LGRIP30)
    - Land use (GLCLUC)
    - DEM (MERIT)
    - Soil parameters (SOILGRIDS 2.0)
    - Soil depth (Pelletier)
- ESRI shapefiles for North American domain:
    - Lakes (HydroLAKES)
    - Geology (GLHYMPS)
 
We want:
- GeoTIFFs at consistent resolutions

We need to:
    - Find highest spatial resolution in existing GeoTIFFs
    - Resample existing GeoTIFFs to this resolution
    - Convert shapefiles to GeoTIFFs at this resolution
    
Update:
	- Highest resolution takes forever with later processing, and information gains are uncertain
	- Added code to resample to lower resolutions