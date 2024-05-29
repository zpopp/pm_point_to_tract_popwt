##Author:         Zach Popp 
##Date Created:   2/1/2024
##Last Modified:  2/1/2024
##       Purpose: Take tract-level estimates (by state) and aggregate to nationwide
##                dataset.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (PM25_Process_Blocks.R)
## 3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 4) Combine state-level tract shapefiles into nationwide tract dataset (Nationwide_Join.R; this file)

library(sf)

# Create list of all files
#
tract_dir <- "" # This is the directory where you have output the tract-level PM2.5 measures
tract_files <- dir(tract_dir, full.names=TRUE, pattern="pm25_PopWt_tl10_.*.Rds")

# Looping through files to merge into one file with all years included
#
for (i in 1:length(tract_files)) {
  # Include indicator of where processing stands
  #
  cat("Processing", i, "of", 49, "files \n")
  
  # Read in file
  #
  input <- readRDS(tract_files[i])
  
  # If first file, initiate final dataset. All other files are added to final dataset
  if (i == 1) { final <- input; next }
  final <- rbind(final, input)
}

#Export rds 
#
saveRDS(final, paste0(tract_dir, "/pm25_PopWt_tl10_2019_Nationwide_Tract.rds"))


