##Author: Zach Popp 
##Date: 05/29/2024
##Last Modified:  NA
##Version Hist:  NA
##Purpose: Join nationwide pm2.5 measures across states and merge with shapefiles
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (02a_PM25_Process_Blocks.R)
## 3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R)
## 4) Use UID-block combination to join pm2.5 for other years to the census blocks
##    without re-running the spatial processing (04_PM25_Join_AllYrs.R)
## 5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
## 6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R; this file)

library(tigris)
library(sf)
library(dplyr)

# Set FIPS directory
#
fips_dir <- "" # directory where state FIPS code list is stored (only required if
               # using bash script)
pm_dir <- "" # directory where state-specific PM2.5 data are stored for block groups, tracts, counties
outdir_nationwide <- "" # directory to output nationwide joined data

# Reading in stateFIPS to get list of FIPS
#
stateFIPS <- read.csv(paste0(fips_dir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE)
stateFIPS <- stateFIPS %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
stateFIPS <- stateFIPS$StFIPS     #integer; does not contain leading zeroes

# Use for loop to read in all nationwide block groups
#
for (state_in in c(stateFIPS)) {
  
  # Read in state block groups
  #
  block_group_sf <- block_groups(year = 2010, state = state_in)
  
  # Read in state tracts
  #
  tract_sf <- tracts(year = 2010, state = state_in)
  
  # Join to nationwide shape
  #
  if (state_in == stateFIPS[1]) {
    bg_nationwide <- block_group_sf
    tract_nationwide <- tract_sf
  } else {
    bg_nationwide <- rbind(bg_nationwide, block_group_sf)
    tract_nationwide <- rbind(tract_nationwide, tract_sf)
  }
  
}

# Read in nationwide counties
#
county_sf <- counties(year = 2010)

# Remove HI and AK from counties
#
county_sf <- filter(county_sf, !STATEFP10 %in% c("02", "15"))

# Combine files into single nationwide dataset
#
files_BG <- dir(path = paste0(pm_dir, "BlockGroup_Wt_10"), full.names=TRUE, pattern=".*pm25_PopWt_2010.*.Rds")
files_Tract <- dir(path = paste0(pm_dir, "Tract_Wt_10"), full.names=TRUE, pattern=".*pm25_PopWt_2010.*.Rds")
files_County <- dir(path = paste0(pm_dir, "County_Wt_10"), full.names=TRUE, pattern=".*pm25_PopWt_2010.*.Rds")

# Merge all block group files into one layer
#
final_BG_nationwide <- do.call(rbind, lapply(files_BG, readRDS))
final_BG_nationwide <- final_BG_nationwide %>%
  rename(GEOID10 = GEOID_BG)

# Merge all census tract files into one layer
#
final_Tract_nationwide <- do.call(rbind, lapply(files_Tract, readRDS))
final_Tract_nationwide <- final_Tract_nationwide %>%
  rename(GEOID10 = GEOID_Tract)

# Merge all county files into one layer
#
final_County_nationwide <- do.call(rbind, lapply(files_County, readRDS))
final_County_nationwide <- final_County_nationwide %>%
  rename(GEOID10 = GEOID_County)

# Join merged data with shapefiles from tigris
#
bg_nationwide_join <- left_join(bg_nationwide, final_BG_nationwide, by = "GEOID10")

# Join merged data with shapefiles from tigris
#
tract_nationwide_join <- left_join(tract_nationwide, final_Tract_nationwide, by = "GEOID10")

# Join merged data with shapefiles from tigris
#
county_nationwide_join <- left_join(county_sf, final_County_nationwide, by = "GEOID10")

# Confirm that no PM2.5 measurements were lost in joining to tigris data
#
if (length(which(!is.na(final_BG_nationwide$pm25_2000))) == length(which(!is.na(bg_nationwide_join$pm25_2000))) &
    length(which(!is.na(final_Tract_nationwide$pm25_2000))) == length(which(!is.na(tract_nationwide_join$pm25_2000))) &
    length(which(!is.na(final_County_nationwide$pm25_2000))) == length(which(!is.na(county_nationwide_join$pm25_2000)))) {
  cat(":) No lost PM2.5 measures! \n")
} else {
  cat("PM2.5 measures in shapefile-joined data differ from data.frame \n")
}

# Save outputs as gpkg for future use as shapefile in spatial analyses
#
st_write(bg_nationwide_join, dsn = paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_bg.gpkg"), append = FALSE)
st_write(tract_nationwide_join, dsn = paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_tract.gpkg"), append = FALSE)
st_write(county_nationwide_join, dsn = paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_county.gpkg"), append = FALSE)

# Remove geometry and save as .csv
#
bg_nationwide_join_df <- as.data.frame(bg_nationwide_join)
bg_nationwide_join_df$geometry <- NULL

tract_nationwide_join_df <- as.data.frame(tract_nationwide_join)
tract_nationwide_join_df$geometry <- NULL

county_nationwide_join_df <- as.data.frame(county_nationwide_join)
county_nationwide_join_df$geometry <- NULL

# Save outputs as csv for future use as shapefile in spatial analyses
#
write.csv(bg_nationwide_join_df, row.names = FALSE, paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_bg.csv"))
write.csv(tract_nationwide_join_df, row.names = FALSE, paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_tract.csv"))
write.csv(county_nationwide_join_df, row.names = FALSE, paste0(outdir_nationwide, "pm25_popwt_tl10_2000to2019_nationwide_county.csv"))
