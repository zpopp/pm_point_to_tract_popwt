##Author: Zach Popp 
##Date: 5/29/2024
##Last Modified:  NA
##Version Hist:  NA
##Purpose: Joining PM2.5 estimates from Joel Schwartz across years into one large file
##Overview: Script joins all of the years of PM2.5 data from 2000 to 2018 into one file
##          based on the uid for each measure. Note: This script only needs to be run
##          once! It does not depend on a specific census geography!
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (02a_PM25_Process_Blocks.R)
## 3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R; this file)
## 4) Use UID-block combination to join pm2.5 for other years to the census blocks
##    without re-running the spatial processing (04_PM25_Join_AllYrs.R)
## 5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R)

library(dplyr)

# Set up directories
#
pm_rawdir <- "" # File where raw PM2.5 components have been stored. Established
                # in script 1 (outdir from that script)
outdir <- "" # Location where PM2.5 joined from 2000 to 2018 will be stored

# Reading in PM2.5 Data
#
PM2.5_files <- dir(pm_rawdir, recursive=TRUE, full.names=TRUE, pattern=".*pm25_total_components_20.*rds")

# Looping through files to merge into one file with all years included
for (i in 1:19) {
  
  #Read in a file for a given year
  input <- readRDS(PM2.5_files[i])
  
  if (i == 1) { final <- input; next }
  
  #Join each new year to the previous years
  final <- left_join(final, input[,3:4], by="uid")
}

# Output final dataset
saveRDS(final, paste0(outdir, "PM25_2000to2018_uid.rds"))
