##Author:       Zach Popp (contrib by Muskaan Khemani)
##Date Created: 10/13/2023
##Last Modified:02/01/2024
##Version Hist: Initially developed for region subset, then modified for nationwide processing.
##              Updated in Jan 2024 to reduce processing with direct .gpkg output
##Purpose:      Merging Mean PM2.5 from Joel Schwartz
##              Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban 
##              and 1km Non-Urban Area Grids for Contiguous U.S., v1 (2000–2019)
##              https://sedac.ciesin.columbia.edu/data/set/aqdh-pm2-5-component-ec-nh4-no3-oc-so4-50m-1km-contiguous-us-2000-2019/data-download
##Note:         For one year, this processing uses ~45-50GB of memory. This only
##              needs to be run once as it aggregates PM2.5 that will be used for 
##              all states and years.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (PM25TotalComponents.R; this file)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (PM25_Process_Blocks.R)
## 3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 4) Combine state-level tract shapefiles into nationwide tract dataset (Nationwide_Join.R)

library(sf)
library(dplyr)
library(data.table)

# The following variables come from the command line when running as bash. Bash
# scripting is a method for expediting processing using a computing cluster.
# To use this code, you would need to separately write a bash script that specifies
# a series of indices that will be used to process multiple states simultaneously
#
# These inputs are only relevant if aiming to process multiple years of PM2.5 data
# For the data shared on Harvard Dataverse, only 2019 was used. To replicate this 
# processing, the line below can be used and the args lines removed.
# year <- 2019
#
args <- commandArgs(trailingOnly = TRUE)
year <- as.numeric(args[1]) # year

# %%%%%%%%%%%%%%%%%%%% IDENTIFY YEAR OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #

# Set directory for reading in Raw Data based on year.
# These scripts assume that the PM2.5 raw data from SEDAC for all components in
# urban and non-urban areas has been downloaded to a local directory. These files
# in total represent ~20GB of data. Download will likely require access to a 
# cloud storage environment. 
#
indir <- paste0("") # directory where raw PM2.5 data downloaded from SEDAC is located
                    # if processing multiple years of data, the paste0 statement
                    # can be used to specify the year of data if listed in the directory
outdir <- paste0("") # directory where PM2.5 will be stored after aggregation across
                     # urban and non-urban areas and across components

# Read in data for each PM2.5 component (urban data)
# Each line of code reads in a distinct PM2.5 components (Elemental Carbon (EC),
# Ammonium (NH4+), Nitrate (NO3-), Organic Carbon (OC), Sulfate (SO42-))
# This section reading in urban data. The files are stored separately as the data
# resolution is 50m for urban settings and 1km for non-urban settings.
#
ec_urban <- readRDS(paste0(indir, "aqdh_pm25component_ec_",year,"_urban.rds"))
nh4_urban <- readRDS(paste0(indir, "aqdh_pm25component_nh4_",year,"_urban.rds"))
no3_urban <- readRDS(paste0(indir, "aqdh_pm25component_no3_",year,"_urban.rds"))
so4_urban <- readRDS(paste0(indir, "aqdh_pm25component_so4_",year,"_urban.rds"))
oc_urban <- readRDS(paste0(indir, "aqdh_pm25component_oc_",year,"_urban.rds"))

# Read in data for each PM2.5 component (nonurban data)
# Each line of code reads in a distinct PM2.5 components (see desc. above)
# This section reading in non-urban data (1km resolution)
#
ec_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_ec_",year,"_non_urban.rds"))
nh4_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_nh4_",year,"_non_urban.rds"))
no3_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_no3_",year,"_non_urban.rds"))
so4_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_so4_",year,"_non_urban.rds"))
oc_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_oc_",year,"_non_urban.rds"))

# Binding together all components PM2.5 measurement
#
# Automated QC check: confirm that the lat/lon match between all of the input datasets
#
if (!isTRUE(all.equal(ec_urban$lat, nh4_urban$lat) & all.equal(ec_urban$lat, no3_urban$lat) &
            all.equal(ec_urban$lat, so4_urban$lat) & all.equal(ec_urban$lat, oc_urban$lat) &
            all.equal(ec_urban$lon, nh4_urban$lon) & all.equal(ec_urban$lon, no3_urban$lon) &
            all.equal(ec_urban$lon, so4_urban$lon) & all.equal(ec_urban$lon, oc_urban$lon))) {
  cat("ERROR: lat/lon do not align between 1 or more input data frames \n") } else {
    cat(":) all lat/lon values align between the input data frames \n")
  }

# Binding urban data
#
final_data_urban <- cbind(ec_urban, 
                          nh4_urban[,"final.predicted.nh4", drop = FALSE], 
                          no3_urban[,"final.predicted.no3", drop = FALSE],
                          so4_urban[,"final.predicted.so4", drop = FALSE],
                          oc_urban[,"final.predicted.oc", drop = FALSE])
# Binding non-urban data
#
final_data_nonurban <- cbind(ec_nonurban,
                             nh4_nonurban[,"final.predicted.nh4", drop = FALSE], 
                             no3_nonurban[,"final.predicted.no3", drop = FALSE],
                             so4_nonurban[,"final.predicted.so4", drop = FALSE],
                             oc_nonurban[,"final.predicted.oc", drop = FALSE])

# Clear memory by removing data frames that are no longer needed
#
rm(ec_urban, nh4_urban, no3_urban, so4_urban, oc_urban,
   ec_nonurban, nh4_nonurban, no3_nonurban, so4_nonurban, oc_nonurban)
gc()

# Automated QC: confirm column names match
#
if (!isTRUE(all.equal(names(final_data_urban), names(final_data_nonurban)))) {
  cat("ERROR: different variable names between urban and nonurban data frames \n") } else {
    cat(":) variable names match between urban and nonurban data frames \n")
  }

# Bind urban and nonurban data
#
final_data_pm25 <- rbind(final_data_urban, final_data_nonurban)

# Add year variable to allow for variable naming
#
final_data_pm25$year <- year

# Use dplyr to sum across components, select variables of interest, and rename based on year
#
final_data_pm25 <- final_data_pm25 %>%
  mutate(total_pm25 = final.predicted.ec + final.predicted.nh4 + final.predicted.no3 + final.predicted.so4 + final.predicted.oc) %>%
  select(lon, lat, year, total_pm25) %>%
  rename_with(~ paste0("pm25_", year), total_pm25)

# Create UID to be used to join PM2.5 data from different years without spatial joins
# The UID is not required for processing a single year, but can be used for later processing.
# With a UID in the dataset, years 2000 to 2018 would not need to be spatially 
# joined to blocks, and can instead be merged to the block level by UID.
# 
final_data_pm25$uid <- c(1:length(final_data_pm25$year))

# Subset to relevant variables
#
final_data_pm25 <- final_data_pm25 %>%
  select(lon, lat, paste0("pm25_", year), uid)

# Save RDS and CSV
#
saveRDS(final_data_pm25, paste0(outdir, "pm25_total_components_", year, ".rds"))

# Convert final_data_pm25 to spatial points data frame for 2019. This only needs to be
#       done for one year, as only one spatial join is conducted and all other years
#       are joined based on the point UID set up in this script.
#       Convert to WGS84 CRS because this is geodetic model stated in metadata,
#       then output as .gpkg which can be trimmed to an input extent using
#       SQL query filter
#
if (year == 2019) {
  final_data_pm25_sf <- st_as_sf(final_data_pm25, coords = c("lon", "lat"), crs = 4326) 
  st_write(final_data_pm25_sf, paste0(outdir, "pm25_total_components_", year, ".gpkg"))
}

