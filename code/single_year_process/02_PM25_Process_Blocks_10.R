##Author:         Zach Popp (contrib. by Dennis Milechin, Keith Spangler)
##Date Created:   10/10/2023
##Last Modified:  2/1/2024
##Version Hist:   Updated in Jan 2024 to run for 2010 blocks
##Purpose:        Joining PM2.5 estimates from Joel Schwartz group to census block geographies.
##                Joining PM2.5 to 2010 block geographies. If processing multiple years,
##                additional data can be separately joined to the 2010 points by UID after
##                the intersection of blocks/points in this script is conducted. This will
##                reduce processing time as spatial joins are more computationally intensive
##                then a join by attribute (eg: uid)
##                PM2.5 assessed at 50m by 50m grid in urban and 1km by 1km grid in nonurban settings. 
##                 Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban 
##                 and 1km Non-Urban Area Grids for Contiguous U.S., v1 (2000–2019)
##                 https://sedac.ciesin.columbia.edu/data/set/aqdh-pm2-5-component-ec-nh4-no3-oc-so4-50m-1km-contiguous-us-2000-2019/data-download
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (PM25_Process_Blocks.R; this file)
## 3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 4) Combine state-level tract shapefiles into nationwide tract dataset. (Nationwide_Join.R)

library(sf)
library(dplyr)
library(data.table)
library(tigris)

# The following variables come from the command line when running as bash. Bash
# scripting is a method for expediting processing using a computing cluster.
# To use this code, you would need to separately write a bash script that specifies
# a series of indices that will be used to process multiple states simultaneously
#
# These inputs are only relevant if aiming to process multiple states of PM2.5 data.
# If aiming to process a single state or county, the command arguments can be removed
# and a line specifying the stateFIPS can be used.
#
args <- commandArgs(trailingOnly = TRUE)
b <- as.numeric(args[1]) # state FIPS index; b=8 for DC

# Create directories to read in data from
#
pm25_dir <- "" # directory where PM2.5 after aggregation across components is stored.
               # should match outdir from Script 1
outdir <- ""   # directory where intersection of PM2.5 and block data will be stored.
fips_dir <- "" # directory where state FIPS code list is stored (only required if
               # using bash script)
  
# %%%%%%%%%%%%%%%%%%%% IDENTIFY STATE FIPS OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #
# Reading in stateFIPS to allow for filtering by state in bash script
#
# This file should include all State FIPS codes. 
# A state FIPS file can be downloaded from https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt
# PM2.5 measures only cover CONUS. Non-CONUS FIPS should be removed as AK and HI are below.
# 
stateFIPS <- read.csv(paste0(fips_dir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE) 
stateFIPS <- stateFIPS %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
stateFIPS <- stateFIPS$StFIPS     #integer; does not contain leading zeroes
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")

# Reading in blocks for relevant state based on command line variable using tigris
#     
blocks <- tigris::blocks(state = stateFIPS, year = 2010)

# We want to make sure the block coordinate system matches the CRS (WGS84) used for the 
# PM2.5 data, so we transform the shapefile. We also want to make sure the geometry
# is validated.
#
blocks <- st_transform(blocks, crs = "EPSG:4326")
blocks <- st_make_valid(blocks)

# Let's create a bounding box polygon for the extent of the state
# and use that to load only the PM2.5 data for that region.
# Only done once, as state boundaries will be consistent over time
# Extract the bounding box parameters
#
extent <- st_bbox(blocks)

# Add approx. 1-kilometer buffer around the extent
#
extent[["xmin"]] <- extent[["xmin"]] - 0.01
extent[["ymin"]] <- extent[["ymin"]] - 0.01
extent[["xmax"]] <- extent[["xmax"]] + 0.01
extent[["ymax"]] <- extent[["ymax"]] + 0.01

# Create a polygon representing the bounding box
#
extent_poly <- st_as_sfc(extent, crs=4326)

# We can create a SQL query filter to only load tracts associated with a FIPS code
# convert the state extent polygon into WKT text
#
wkt <- st_as_text(extent_poly)

# Use WKT to filter the data being loaded from the GeoPackage file.
# Note: The year of data being used here is arbitrary
#       because the lat/lon points are the same for all years of the model. The
#       uids which have been created can be used to join other years to the blocks
#
pm25_tot_comp_sf <- st_read(paste0(pm25_dir, "2019/pm25_total_components_2019.gpkg"), layer="pm25_total_components_2019", wkt_filter=wkt)

# Specify column names in case the data order changes in the future
#
pm25_tot_comp_sf <- pm25_tot_comp_sf[,c("uid", "pm25_2019", "geom")] 

# Generate 2-digit year for naming purposes
#
yr <- "10"

# Subset to only relevant block columns
#
GEOID <- names(blocks)[grep("^GEOID|BLKID", names(blocks), ignore.case = TRUE)]
blocks <- blocks[c(GEOID)]

# Transform to WGS84 to match the census shapefile
#
pm25_tot_comp_sf <- st_transform(pm25_tot_comp_sf, crs = st_crs(blocks))

# Apply the intersection of blocks and PM2.5 points to capture mean PM2.5 across blocks
#
block_PM2.5_intersect <- st_join(blocks, pm25_tot_comp_sf, join=st_intersects)

# Identify blocks that were not intersecting with any PM2.5 data points
#
missing_PM <- block_PM2.5_intersect %>% filter(is.na(uid))

# Subset to correct length for binding in future
#
missing_PM <- missing_PM[c(GEOID)]

# Identify blocks that were intersecting with PM2.5 data points
#
pm_complete <- block_PM2.5_intersect %>% filter(!is.na(uid))

# For blocks not intersecting, use st_nearest_feature to assign PM2.5 for non-intersecting, but closest point
#
missing_PM <- st_join(missing_PM, pm25_tot_comp_sf, join=st_nearest_feature)

# Set both datasets(st_intersects joined and st_nearest_feature joined) as data.table for faster processing
#
setDT(pm_complete)
setDT(missing_PM)

# Bind st_nearest and st_intersects joined data 
# QC to check that pm_complete and missing_PM have same number of columns and column names
#
if (!isTRUE(all.equal(names(pm_complete), names(missing_PM)))) { 
  cat("ERROR: mismatch in dimensions"); break }

blocks_merge <- rbind(pm_complete, missing_PM)

# Write the file to folder
# This output will have a row for each unique PM2.5 point which overlaps with
# the state shapefile. This file can be used to connect other years of PM2.5
# data to the 2019 data joined here. 
#
saveRDS(blocks_merge,
        paste0(outdir, "tl",yr,"bl_PM2.5_", "2019", stateFIPS, ".rds"))

# For this dataset, we only process 2019 data, using the process below
# Apply the summary using data.table. Average by block applied across pm measures
# 
totalPM2.5mean <- blocks_merge[, lapply(.SD, mean, na.rm=TRUE), 
                                    by = GEOID, .SDcols = patterns("^pm25_")]

# Save final output for block-level
# This output will have a row for each census tract, with the mean PM2.5 of all
# intersecting PM2.5 points. It can be used to aggregate estimates of PM2.5 up
# to the census block group, tract, or county by population or area weights
#
saveRDS(totalPM2.5mean, paste0(outdir,"/tl10_bl_PM2.5_mean_2019_",stateFIPS,".rds"))


