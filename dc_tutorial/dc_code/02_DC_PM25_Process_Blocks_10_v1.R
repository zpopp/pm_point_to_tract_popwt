##Author: Zach Popp (contrib. by Dennis Milechin, Keith Spangler)
##Date Created: 10/10/2023
##Data Modified: 12/6/2023
##Purpose: Joining PM2.5 estimates from Joel Schwartz group to census block geographies.
##          Joining PM2.5 to 2019 block geographies which will be used
##          for the years preceding within the same decade (eg: 2010-2019 to 2019 blocks)
##          PM2.5 assessed at 50m by 50m grid in urban and 1km by 1km grid in nonurban settings. 
##Overview: Read in census blocks for bash-specified state. Crop PM2.5 data to extent
##          of state while reading the data in (using SQL query). Use for loop to join
##          2019 PM2.5 data to blocks for 2009 and 2019 census geographies. To conduct
##          join, use st_intersects to assign block-level PM2.5 where PM2.5 grid overlaps
##          with blocks. Use st_nearest to assign block-level PM2.5 where grid does not
##          overlap. Join datasets and output for later use. 

library(sf)
library(dplyr)
library(data.table)
library(tigris)

# The following variables come from the command line when running as bash. We 
# have commented out the bash lines and added inputs relevant to the tutorial.
# For more information about bash scripts, see Base Scripting tutorial (link)
#
#args <- commandArgs(trailingOnly = TRUE)
#b <- as.numeric(args[1]) # state FIPS index; b=8 for DC
b <- 8
year <- 2010 # year

# Create directories to read in data from
#
indir <- "dc_tutorial/dc_rawdata"
outdir <- "dc_tutorial/dc_outputata"

# %%%%%%%%%%%%%%%%%%%% IDENTIFY STATE FIPS OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #
# Reading in stateFIPS to allow for filtering by state in bash script
# We here use an index (b) to read in the state we want to process (DC). 
# This index is used because when processing nationwide data, we would want to 
# process each state separately due to the large files and computational processing
# involved. Since we are using one state, the below could be replaced with:
#       stateFIPS <- "11"
#
stateFIPS <- states(year = 2010)
stateFIPS <- stateFIPS %>% filter(STUSPS10 != "AK" & STUSPS10 != "HI") %>%
  arrange(GEOID10)
stateFIPS <- stateFIPS$STATEFP10     #integer; does not contain leading zeroes
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")

# Reading in blocks for relevant state based on command line variable using tigris
#     
blocks <- tigris::blocks(state = stateFIPS, year = 2010)

# We want to make sure the block coordinate system matches the CRS used for the 
# PM2.5 data, so we transform the shapefile. To check the CRS of a file, the 
# st_crs() command can be used.
# We also want to make sure the geometry is validated as having points of 
# intersection in unexpected locations can cause spatial processing to fail.
#
blocks <- st_transform(blocks, crs = "EPSG:4326")
blocks <- st_make_valid(blocks)

# We create a bounding box polygon for the extent of the state
# and use that to load only the PM2.5 data for that region.
# This step is only done once even though we are going to use blocks for 
# multiple years, as state boundaries will be consistent over time
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
# The WKT filter here reads in only the extent of the input file that is within
# the specified extent_poly from the line above. This is not really applicable
# to the tutorial since we have created a PM2.5 file that is already restricted
# to the DC area, but included here as an example of what could be used if 
# working with the nationwide PM2.5 data.
# Note: The year of data being used here is arbitrary
#       because the lat/lon points are the same for all years of the model. The
#       uids which have been created will be used to join other years to the blocks
#       if processing multiple years.
#
pm25_tot_comp_sf <- st_read(paste0(indir, "pm25_total_components_2010.gpkg"), layer="pm25_total_components_2010", wkt_filter=wkt)

# Specify column names in case the data order changes in the future
#
pm25_tot_comp_sf <- pm25_tot_comp_sf[,c("uid", "pm25_2010", "geom")] 

# Generate 2-digit year for naming purposes
#
yr <- substr(year, 3, 4)

# Subset to only relevant block columns. The grep() function here is used to 
# extract any column names with either GEOID or BLKID. This was inserted as 
# we may want to process data from 2000 to 2019, and in doing so, would work
# with different unique block identifiers.
#
GEOID <- names(blocks)[grep("^GEOID|BLKID", names(blocks), ignore.case = TRUE)]
blocks <- blocks[c(GEOID)]

# Re-project to NAD83 to match the census shapefile
#
pm25_tot_comp_sf <- st_transform(pm25_tot_comp_sf, crs = st_crs(blocks))

# Apply the intersection of blocks and PM2.5 points to capture mean PM2.5 across blocks
#
block_PM2.5_intersect <- st_join(blocks, pm25_tot_comp_sf, join=st_intersects)

# Identify blocks that were not intersecting with any PM2.5 data points
#
missing_PM <- block_PM2.5_intersect %>% filter(is.na(uid))

# Subset data to single column. After the intersect operation which failed for the 
# missing blocks, the column names from the PM2.5 data were still appended. We want
# to rejoin the PM2.5 to these blocks using the st_nearest_feature function, but
# first should adjust so the columns only represent those for blocks. This will
# also allow us to combine these datasets into one file more easily.
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
# data to the 2010 data joined here. 
#
saveRDS(blocks_merge,
        paste0(outdir, "tl",yr,"_bl_PM2.5_", "2010_", stateFIPS, ".rds"))

# For this dataset, we only process 2010 data. We have connected each block to the
# PM2.5 points that intersect or are nearest. For some blocks, more than 1 
# PM2.5 point will be intersected. We will apply the mean function below
# to get a single PM2.5 measure for each block
# 
totalPM2.5mean <- blocks_merge[, lapply(.SD, mean, na.rm=TRUE), 
                                    by = GEOID, .SDcols = patterns("^pm25_")]

# Save final output for block-level
# This output will have a row for each census block, with the mean PM2.5 of all
# intersecting PM2.5 points. It can be used to aggregate estimates of PM2.5 up
# to the census block group, tract, or county by population or area weights
#
saveRDS(totalPM2.5mean, paste0(outdir,"/tl10_bl_PM2.5_2010_mean_",stateFIPS,".rds"))


