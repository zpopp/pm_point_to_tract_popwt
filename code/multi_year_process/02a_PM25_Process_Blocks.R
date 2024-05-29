##Author:         Zach Popp (contrib. by Dennis Milechin, Keith Spangler)
##Date Created:   10/10/2023
##Last Modified:  2/1/2024
##Version Hist:   Updated in Jan 2024 to run for 2010 blocks
##Purpose:        Joining PM2.5 estimates from Joel Schwartz group to census block geographies.
##                Joining PM2.5 to 2010 block geographies. Additional data will be separately 
##                joined to the 2010 points by UID after the intersection of blocks/points in this script is conducted. This will
##                reduce processing time as spatial joins are more computationally intensive
##                then a join by attribute (eg: uid)
##                PM2.5 assessed at 50m by 50m grid in urban and 1km by 1km grid in nonurban settings. 
##                 Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban 
##                 and 1km Non-Urban Area Grids for Contiguous U.S., v1 (2000–2019)
##                 https://sedac.ciesin.columbia.edu/data/set/aqdh-pm2-5-component-ec-nh4-no3-oc-so4-50m-1km-contiguous-us-2000-2019/data-download
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (02a_PM25_Process_Blocks.R; this file)
## 3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R)
## 4) Use UID-block combination to join pm2.5 for other years to the census blocks
##    without re-running the spatial processing (04_PM25_Join_AllYrs.R)
## 5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R)

library(sf)
library(dplyr)
library(data.table)

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
b <- as.numeric(args[1]) # state FIPS index

# Set year to link longitudinal PM2.5 measures to relevant census blocks
# Note: Census block data for the specified year will need to be available in
# the specified directories. Check that these are available before running the script
# or it will fail
#
decyear <- 2010
deyr <- substr(decyear, 3, 4)

# Create directories to read in data from
#
fips_dir <- "" # directory where state FIPS code list is stored (only required if
               # using bash script)
bl_indir <- "" # directory where block shapefiles are stored if already downloaded. 
            # if not downloaded, see pm_point_to_tract_popwt/code/02_PM25_Process_Blocks_10.R
            # for code to download from tigris
pm_indir <- "" # directory where PM2.5 after aggregation across components is stored.
             # should match outdir from Script 1
pm_outdir <- # directory where intersection of PM2.5 and block data will be stored.

# %%%%%%%%%%%%%%%%%%%% IDENTIFY STATE FIPS OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #
# Reading in stateFIPS to allow for filtering by state in bash script
#
stateFIPS <- read.csv(paste0(fips_dir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE)
stateFIPS <- stateFIPS %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
stateFIPS <- stateFIPS$StFIPS     #integer; does not contain leading zeroes
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")

# Reading in blocks for relevant state based on command line variable
#
tl_blocks <- st_read(paste0(bl_indir, "tl_", decyear, "_", stateFIPS, "_tabblock", deyr, ".shp"))
tl_blocks <- st_make_valid(tl_blocks)

# Subset to only relevant block columns
#
GEOID <- names(tl_blocks)[grep("^GEOID|BLKID", names(tl_blocks), ignore.case = TRUE)]
tl_blocks <- tl_blocks[c(GEOID)]

# Let's create a bounding box polygon for the extent of the state
# and use that to load only the PM2.5 data for that region.
# Only done once, as state boundaries will be consistent over time
# Extract the bounding box parameters
#
extent <- st_bbox(tl_blocks)

# Add approx. 1-kilometer buffer around the extent
#
extent[["xmin"]] <- extent[["xmin"]] - 0.01
extent[["ymin"]] <- extent[["ymin"]] - 0.01
extent[["xmax"]] <- extent[["xmax"]] + 0.01
extent[["ymax"]] <- extent[["ymax"]] + 0.01

# Create a polygon representing the bounding box
#
extent_poly <- st_as_sfc(extent, crs = st_crs(tl_blocks))

# We can create a SQL query filter to only load tracts associated with a FIPS code
# convert the polygon into WKT text
#
wkt <- st_as_text(extent_poly)

# We can create a SQL query filter to only load tracts associated with a FIPS code
# convert the state extent polygon into WKT text
#
pm25_tot_comp_sf <- st_read(paste0(pm_indir, "pm25_total_components_2019.gpkg"), layer="pm25_total_components_2019", wkt_filter=wkt)

gc() # free up memory

# If the pm25_2019 loads as string, convert it to numeric.
#
pm25_tot_comp_sf$pm25_2019 <- as.numeric(pm25_tot_comp_sf$pm25_2019)

# Use WKT to filter the data being loaded from the GeoPackage file.
# Note: The year of data being used here is arbitrary
#       because the lat/lon points are the same for all years of the model. The
#       uids which have been created can be used to join other years to the blocks
#
pm25_tot_comp_sf <- st_as_sf(pm25_tot_comp_sf, coords=c("lon", "lat"), # Double check that lat/lon are labeled correctly
                          crs = st_crs(tl_blocks))                     # in the input dataset

pm25_tot_comp_sf <- pm25_tot_comp_sf[,c("pm25_2019", "uid", "geom")] # specify column names in case the data order changes in the future

# Re-project to NAD83 to match the census shapefile
#
pm25_tot_comp_sf <- st_transform(pm25_tot_comp_sf, crs = st_crs(tl_blocks))

# Apply the intersection of blocks and PM2.5 points to capture mean PM2.5 across blocks
#
tl_bl_PM2.5_intersect <- st_join(tl_blocks, pm25_tot_comp_sf, join=st_intersects)

# Identify blocks that were not intersecting with any PM2.5 data points
#
missing_PM<- tl_bl_PM2.5_intersect %>% filter(is.na(pm25_2019))

# Identify blocks that were intersecting with PM2.5 data points
#
pm_complete<- tl_bl_PM2.5_intersect %>% filter(!is.na(pm25_2019))

# Subset to correct length for binding in future
#
missing_PM <- missing_PM[c(GEOID)]

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

tl_bl_merge <- rbind(pm_complete, missing_PM)

# Write the file to folder
#
saveRDS(tl_bl_merge,
         paste0(pm_outdir, "tl", deyr, "_bl_2019_PM2.5_", stateFIPS, ".rds"))

