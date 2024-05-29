##Author:       Muskaan Khemani, Zach Popp
##Date Created: 10/13/2023
##Last Modified: 04/30/2024
##Version Hist: 
##Purpose:      Merging Mean PM2.5 from Joel Schwartz
##              Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban 
##              and 1km Non-Urban Area Grids for Contiguous U.S., v1 (2000–2019)
##              https://sedac.ciesin.columbia.edu/data/set/aqdh-pm2-5-component-ec-nh4-no3-oc-so4-50m-1km-contiguous-us-2000-2019/data-download
##Note:         For one year, this processing uses ~45-50GB of memory. This only
##              needs to be run once as it aggregates PM2.5 that will be used for 
##              all states and regardless of census block year.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (TotalComponentsPM25.R; this file)
##    One of the years which were processed in step 1 will need to be saved as a 
##    .gpkg to allow for easier spatial processing (PM25_convert_ogr19.sh)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 DC census blocks.
## 3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)

##Notes on Tutorial:
##    The PM2.5 data used in this tutorial is shared through SEDAC, which is 
##    housed within NASA's Earth Observing System Data and Information System (EOSDIS)
##    Users will need to create an account to download data. For more information
##    on how to download these data directly through R, please see the CHORDS
##    toolkit: https://niehs.github.io/PCOR_bookdown_tools/chapter-nasa-earthdata.html 
##
##    For this section of the GitHub repository on population-weighted PM2.5
##    data, a subset of the data for Washington, D.C. has been cut and shared.
##    This was done to allow users to walk through the steps of processing
##    without needing to use a cloud computing environment. Cloud computing is 
##    needed to conduct nationwide processing because of the size of the files
##    and the burden of spatial aggregation with these large data.

library(sf)
library(dplyr)
library(data.table)

# Enter the year of data to distinguish which raw data to read in
# If you would like to process another year, change the input below. Please note
# that you will need to have the data for that year already downloaded for the 
# processing to occur. For this tutorial, only 2010 data for DC is accessible.
#
year <- 2010 # year

# %%%%%%%%%%%%%%%%%%%% IDENTIFY YEAR OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #

# Set directory for reading in Raw Data and outputting Intermediate Data
#
indir <- "dc_tutorial/dc_rawdata"

# Read in data for each PM2.5 component (urban data)
# Each line of code reads in a distinct PM2.5 components (Elemental Carbon (EC),
# Ammonium (NH4+), Nitrate (NO3-), Organic Carbon (OC), Sulfate (SO42-))
# This section reading in urban data. The files are stored separately as the data
# resolution is 50m for urban settings and 1km for non-urban settings.
# Note that if using files directly from SEDAC, the filename will not have a 
# _dc suffix.
#
ec_urban <- readRDS(paste0(indir, "aqdh_pm25component_ec_",year,"_urban_dc.rds"))
nh4_urban <- readRDS(paste0(indir, "aqdh_pm25component_nh4_",year,"_urban_dc.rds"))
no3_urban <- readRDS(paste0(indir, "aqdh_pm25component_no3_",year,"_urban_dc.rds"))
so4_urban <- readRDS(paste0(indir, "aqdh_pm25component_so4_",year,"_urban_dc.rds"))
oc_urban <- readRDS(paste0(indir, "aqdh_pm25component_oc_",year,"_urban_dc.rds"))

# Read in data for each PM2.5 component (nonurban data)
# Each line of code reads in a distinct PM2.5 components (see desc. above)
# This section reading in non-urban data (1km resolution)
# For this data subset, we are focused on Washington, D.C. All of D.C. is classified
# as urban, so these data do not need to be read in. The code is commented out,
# but may be useful for another area
#
#ec_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_ec_",year,"_non_urban.rds"))
#nh4_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_nh4_",year,"_non_urban.rds"))
#no3_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_no3_",year,"_non_urban.rds"))
#so4_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_so4_",year,"_non_urban.rds"))
#oc_nonurban <- readRDS(paste0(indir, "aqdh_pm25component_oc_",year,"_non_urban.rds"))

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
# See above for information on why nonurban data is removed
#
#final_data_nonurban <- cbind(ec_nonurban,
#                             nh4_nonurban[,"final.predicted.nh4", drop = FALSE], 
#                             no3_nonurban[,"final.predicted.no3", drop = FALSE],
#                             so4_nonurban[,"final.predicted.so4", drop = FALSE],
#                             oc_nonurban[,"final.predicted.oc", drop = FALSE])

# Clear memory by removing data frames that are no longer needed
#
rm(ec_urban, nh4_urban, no3_urban, so4_urban, oc_urban)

#rm(ec_urban, nh4_urban, no3_urban, so4_urban, oc_urban,
#   ec_nonurban, nh4_nonurban, no3_nonurban, so4_nonurban, oc_nonurban)

# Automated QC: confirm column names match (only applicable when using nonurban data)
#
#if (!isTRUE(all.equal(names(final_data_urban), names(final_data_nonurban)))) {
#  cat("ERROR: different variable names between urban and nonurban data frames \n") } else {
#    cat(":) variable names match between urban and nonurban data frames \n")
#  }

# Bind urban and nonurban data
# Not applicable
#
#final_data_pm25 <- rbind(final_data_urban, final_data_nonurban)

# Add year variable to allow for variable naming
#
final_data_urban$year <- year

# Use dplyr to sum across components, select variables of interest, and rename based on year
#
final_data_urban <- final_data_urban %>%
  mutate(total_pm25 = final.predicted.ec + final.predicted.nh4 + final.predicted.no3 + final.predicted.so4 + final.predicted.oc) %>%
  select(lon, lat, year, total_pm25) %>%
  rename_with(~ paste0("pm25_", year), total_pm25)

# Create UID to be used to join PM2.5 data from different years without spatial joins
# 
final_data_urban$uid <- c(1:length(final_data_urban$year))

# Subset to relevant variables
#
final_data_urban <- final_data_urban %>%
  select(lon, lat, paste0("pm25_", year), uid)

# Save RDS and CSV
#
saveRDS(final_data_urban, paste0(outdir, "pm25_total_components_", year, ".rds"))

# Convert final_data_pm25 to spatial points data frame for 2019. This only needs to be
#       done for one year, as only one spatial join is conducted and all other years
#       are joined based on the point UID set up in this script.
#       Convert to WGS84 CRS because this is geodetic model stated in metadata,
#       then output as .gpkg which can be trimmed to an input extent using
#       SQL query filter
#
final_data_urban_sf <- st_as_sf(final_data_urban, coords = c("lon", "lat"), crs = 4326) 
st_write(final_data_urban_sf, paste0(indir, "pm25_total_components_", year, ".gpkg"), append=FALSE)

