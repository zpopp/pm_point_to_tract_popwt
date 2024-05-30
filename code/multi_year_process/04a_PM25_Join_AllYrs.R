##Author: Zach Popp 
##Date: 10/10/2023
##Last Modified:  NA
##Version Hist:  NA
##Purpose: Merging Mean PM2.5 from Joel Schwartz 2000-2019 to nationwide blocks.
##          Total components have been summed from raw data (see PM25_TotComp_AllYrs)
##          for details. Years 2000 to 2018 were then merged to a single .RDS file
##          (see PM25_Join_2000to2018.R)
##
##          PM2.5 total components for 2019 were joined to 2010 and 2020 block polygons
##          (see PM25_Process_Blocks.R for details). A UID was maintained in the PM2.5 data
##          to allow for merging of additional years without computational intensity of spatial
##          join. Run by state in bash. For loop for all states in one script crashed.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (02a_PM25_Process_Blocks.R)
## 3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R)
## 4) Use UID-block combination to join pm2.5 for other years to the census blocks
##    without re-running the spatial processing (04_PM25_Join_AllYrs.R; this file)
## 5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R)

library(sf)
library(dplyr)
library(data.table)

# The following variables come from the command line when running as bash
# The inputs should be 1-49 to run for CONUS states
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

# Set up directories 
#
fips_dir <- "" # directory where state FIPS code list is stored (only required if
               # using bash script)
census_indir <- "" # directory where block shapefiles are stored if already downloaded. 
                   # if not downloaded, see pm_point_to_tract_popwt/code/02_PM25_Process_Blocks_10.R
                   # for code to download from tigris
pm_outdir <- "" # directory where pm2.5 merged to blocks for the initial year from 
                # script 2_PM25_Process_Blocks.R is stored, and where outputs
                # for all merged years will be output
pm_indir <- "" # directory where PM2.5 joined for all years from script 3_PM25_Join_2000to2018.R
               # is stored

# %%%%%%%%%%%%%%%%%%%% IDENTIFY STATE FIPS OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #
# Reading in stateFIPS to allow for filtering by state in bash script
#
stateFIPS <- read.csv(paste0(fips_dir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE)
stateFIPS <- stateFIPS %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
stateFIPS <- stateFIPS$StFIPS     #integer; does not contain leading zeroes
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")

# Read in relevant 2019 PM2.5 joined block shapefile based on command line input
#
blocks <- readRDS(paste0(pm_outdir, "tl", deyr, "_bl_2019_PM2.5_",stateFIPS,".rds"))

# Read in all PM2.5 data across years
#
pm25_2000to18 <- readRDS(paste0(pm_indir, "PM25_2000to2018_uid.rds"))

# Save GEOID from blocks dataset
#
GEOID <- names(blocks)[grep("^GEOID",  names(blocks))]

# Set blocks as data.frame
#
blocks <- as.data.frame(blocks)

# Save only relevant columns
#
blocks <- blocks[, c(GEOID, "pm25_2019", "uid", "geometry")]

# Reading in statewide polygon object and evaluating extent so as to trim size of PM2.5 (reduced 
# computational burden of merge). Extent equivalent across years as full state extent not expected to change)
#
blocks <- st_read(paste0(census_indir, "tl_", decyear, "_", stateFIPS, "_tabblock", deyr, ".shp"))
extent <- st_bbox(blocks_ma)

# Filtering PM2.5 data to size of the state being processed
#
pm25_2000to18 <- pm25_2000to18 %>% filter(lon>=extent$xmin&lon<=extent$xmax&lat>=extent$ymin&lat<=extent$ymax)

# Set uid to be numeric to allow join
#
blocks$uid <- as.numeric(blocks$uid)

# Conduct merge by uid, ensure naming of data is consistent across years
#
merged_totalPM2.5 <- left_join(blocks, pm25_2000to18, by="uid")

# Set data.table to allow dt summary
#
setDT(merged_totalPM2.5)

# Apply the summary using data.table. Average by block applied across pm measures
#
totalPM2.5sum <- merged_totalPM2.5[, lapply(.SD, mean), 
                                           by = GEOID10, .SDcols = patterns("^pm25_")]

# Save final outputs - includes all years in one file.
#
saveRDS(totalPM2.5sum, paste0(pm_outdir, "tl", deyr, "_bl_PM2.5_allyr_",stateFIPS,".rds"))
