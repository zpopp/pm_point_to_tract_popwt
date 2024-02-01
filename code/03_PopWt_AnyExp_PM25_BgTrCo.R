##Author:         Zach Popp (contrib. by Dennis Milechin, Keith Spangler)
##Date Created:   9/22/2023
##Last Modified:  2/1/2024
##       Purpose: Take block-level estimates (by state and year), and aggregate
##                using population weights up to the block group, census tract,
##                and county levels. The weighting is derived from template code
##                that had been created for PRISM block values, and has been modified
##                to run for PM2.5 variables here based on parameters
##                input at the beginning of the script.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (PM25_Process_Blocks.R)
## 3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R; this file)
## 4) Combine state-level tract shapefiles into nationwide tract dataset (Nationwide_Join.R)
  
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%% OVERVIEW  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
#
# This script has been created to make the processing of exposure data
# from the block-level up to the census tract more interchangeable. Instead of 
# copying, pasting, and re-adjusting scripts, novel exposure data can be processed
# based on adjustment of a handful of inputs within the STEP 1. ESTABLISH DATA INPUTS
# section below. Please read the section carefully before proceeding.
#
# The population weighting process is different for longitudinal data where multiple
# years of data are processed to a single year's census geographies, vs. with cross-sectional or
# time series data where each year of data is joined to the census geographies for
# that year. This script is for data which will be joined to census data for the 
# end of the decade in which the data was collected (year-by-year).
#
# Also, the population weighting code differs for daily v. annual measures. This 
# code is relevant for annual data.
#
library("sf")     # For vector data
library("plyr")
library("tigris") # For downloading census shapefiles
library("doBy")
library("tidyverse")
library("tidycensus")
sf_use_s2(FALSE) # S2 is for computing distances, areas, etc. on a SPHERE (using
# geographic coordinates, i.e., decimal-degrees); no need for this
# extra computational processing time if using PROJECTED coordinates,
# since these are already mapped to a flat surface. Here, PM2.5
# is in geographic coordinates, but the scale of areas we are 
# interested in is very small, and hence the error introduced by 
# ignoring the Earth's curvature over these tiny areas is negligible and
# a reasonable trade off given the dramatic reduction in processing time

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
b <- as.numeric(args[1]) # county FIPS index; b = 290 for DC in 2000 and 291 for 2019/2020 # KRS: I don't think this works with your code since you stratify by state not county. I think DC should be 8

# %%%%%%%%%%%%%%%%%% STEP 1. ESTABLISH DATA INPUTS %%%%%%%%%%%%%%%%%%%%% #
#
# This script is set up to run for distinct exposures, across states, and for a
# variety of years. These inputs can be stated in bash or added directly to the script
# if only a singular year or state is needed. Please review the Step 1 inputs 
# thoroughly to ensure all inputs are properly references, or else the script
# will fail to run.

# %%%%%%%%%%%%%%%%%%%% IDENTIFY STATE FIPS OF INTEREST %%%%%%%%%%%%%%%%%%%%%%% #
# Fips and abbrev relationship, prep for bash scripting
#
fips_dir <- "" # directory where state FIPS code list is stored (only required if
               # using bash script)
# This file should include all State FIPS codes (Only require if using bash script) 
# A state FIPS file can be downloaded from https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt
# PM2.5 measures only cover CONUS. Non-CONUS FIPS should be removed as AK and HI are below.
# 
states <- read.csv(paste0(fipsdir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE)
states <- states %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
states$StFIPS <- formatC(as.numeric(states$StFIPS), width = 2, format = "d", flag = "0")
state_mapping <- states[, c("StFIPS", "StAbbrev")]  # Keep relevant columns
stateFIPS <- states$StFIPS
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")
state_abbrev <- state_mapping$StAbbrev[state_mapping$StFIPS == stateFIPS]

# Set year to be the year of PM2.5 data you are using
#
year <- 2019

# The exp object is meant to be the character string that describes the exposure data
# which you are seeking to population weight. Later in the script, the
# all variables beginning with this prefix will be queried for weighting.
exp <- "pm25"

# For year-by-year data, two 'decennial' inputs are needed. One is for the 
# year of census geographies that will be used (decyear_geo), which will be the
# end of the decade in which the data was collected to represent all changes that
# have been made over the course of the decade. The second is the decennial census 
# for the beginning of the year, which will be used to query the relevant block
# population (decyear_pop)
#
decyear_geo <- ifelse(year >= 2020, 2020,
                  ifelse(year %in% c(2010:2019), 2019,
                         ifelse(year %in% c(2000:2009), 2009, NA)))
decyear_pop <- ifelse(year >= 2020, 2020,
                  ifelse(year %in% c(2010:2019), 2010,
                         ifelse(year %in% c(2000:2009), 2000, NA)))

# The deyr variable is a substring 2-digit representation of the decennial year
# for reading in and outputting files
deyr <- substr(decyear_geo, 3,4)

# Function to calculate sum such that NAs are removed but if all values are NA
# then NA is returned instead of 0.
#
sumfun <- function(x) { return(ifelse(all(is.na(x)), NA, sum(x, na.rm = TRUE))) }

# %%%%%%%%%%%%%%%%%%%%%%% DEFINE DATA DIRECTORIES %%%%%%%%%%%%%%%%%%%%%%%%%%% #

# The outdir is where you would like to save the output data. Subdirectories for
# block group, census tract, and county level measures may be useful, but can be
# input later in the script.
#
outdir <- ""

# The exposure_file is the file containing the block-level measures. The paste0
# function will string together the bash input and decennial year information
# to query the appropriate file
#
exposure_file <- paste0(outdir, "tl10_bl_PM2.5_mean_2019_", stateFIPS, ".rds")

# %%%%%%%%%%%%%%% SPECIFY DESIRED OUTPUT GEOGRAPHIES %%%%%%%%%%%%%%%%%%%%%%% #
# A for loop will be used to population weight data from the census block
# up to each of the geographies listed below. Do not change the names of these
# inputs, but remove any that are unnecessary. 
#
censusgeos <- c("BG", "Tract", "County")
subgeoid <- c("GEOID_BG", "GEOID_Tract", "GEOID_County")

# %%%%%%%%%%%%%%%%%%%% READ IN EXPOSURE FILES %%%%%%%%%%%%%%%%%%%%%%% #
# The filepath here should be specified in the Directory Definition section
# To use this script, inputs with block-level estimates of an exposure that is 
# consistently defined will be needed
#
exp_block <- readRDS(exposure_file)

# Automated QC check to confirm that the exposure data has only one observation per unique block ID
#
exp_block_geoid <- names(exp_block)[grep("^GEOID|^BLKIDFP", names(exp_block))]
if (dim(exp_block)[1] != length(unique(exp_block[[exp_block_geoid]]))) {
  cat("ERROR: number of unique blocks does not equal number of observations in exposure data! \n")
} else { cat(":) number of exposure measurments matches number of unique blocks \n") }

# %%%%%%%%%%%%%%%%%% STEP 2. JOIN DATA to POPULATION COUNTS %%%%%%%%%%%%%%%%%% #
#
# Obtain block-level population data. Note that the Census Bureau only releases
# population counts at the block level in the Decennial Census (2000, 2010, 2020, etc.).
# Note also that block identifiers (i.e., GEOID or FIPS code) can change over time.
# When merging population data with non-Decennial Census years, you may need to 
# account for these changing identifiers.
#
# You can download Census data in a variety of ways. Here, we are using the
# tidycensus package, which uses the Census Bureau's API. You must first obtain 
# an API key. Visit https://www.census.gov/data/developers.html and select the
# large "Request a Key" image on the left side of the screen to request an API key.
# Enter your API key at the top of the script

# Block populations must be queried for each individual county. Identify all of the
# counties in the state being processed and then loop through them
#
counties <- tigris::counties(state = stateFIPS, year = decyear_pop, cb = TRUE)
counties <- unique(counties[[grep("^COUNTYFP", names(counties), ignore.case = TRUE)]])

# Total population variable will vary by Decennial Census
#
variable <- ifelse(year %in% 2000:2009, "PL001001",
                   ifelse(year %in% 2010:2019, "P001001",
                          ifelse(year %in% 2020:2029, "P1_001N", NA)))

blockpop <- get_decennial(geography = "block",
                          variables = variable, # Use load_variables("pl", year = 2010) to see available vars
                          year = decyear_pop,
                          state = stateFIPS,
                          county = counties,
                          sumfile = "pl")

# Automated QC check -- confirm data is total population
#
if (length(which(blockpop[[variable]] != variable)) > 0) {
  cat("ERROR: Not all pop. vars. \n") } else { cat(":) all pop. vars. \n") }

# Rename population variable
#
pop_var_name <- paste0("Pop_", decyear_pop)
names(blockpop)[grep("^value$", names(blockpop), ignore.case = TRUE)] <- pop_var_name
blockpop[,c("NAME", "variable")] <- NULL

# Get the GEOID for the block shapefile and for the block populations file, which may 
# change depending on the year of Census data
#
GEOID_shapefile <- names(exp_block)[grep("^GEOID|BLKID", names(exp_block), ignore.case = TRUE)]
GEOID_popfile <- names(blockpop)[grep("^GEOID|BLKID", names(blockpop), ignore.case = TRUE)]

# Merge the population data with the shapefile
#
exp_block <- merge(exp_block, blockpop, by.x = GEOID_shapefile, by.y = GEOID_popfile, all.x = TRUE)
num_missing <- length(which(is.na(exp_block[[pop_var_name]])))

if (num_missing > 0) {
  cat("WARNING:", num_missing, "blocks with missing population \n") 
} else { cat(":) no blocks with missing population \n") }


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%% STEP 3: AGGREGATE UP FROM BLOCK TO BG, TRACT, AND COUNTY %%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
#
# The final step is to calculate population-weighted averages by variable at the
# levels of block groups (bg), census tracts, and counties. Census blocks nest
# within every other Census boundary; however, the only units in which they nest
# based on their GEOID (i.e., FIPS code) are block group, tract, county, and state.
# Other census units use different administrative boundaries. To aggregate up
# to these units, e.g., zip code tabulation area (ZCTA), you will need to
# use a crosswalk to link each census block to its accompanying ZCTA.
#
# By contrast, the GEOID variable contains the unique identifiers in which the
# block is nested:
#
# GEOID:  110010001011000 = block     (15 characters [+ 1 letter sometimes in intra-decadal years])
#         110010001011 = block group  (12 characters)
#         11001000101 = tract         (11 characters)
#         11001 = county              (5 characters)
#         11 = state                  (2 characters)
#
# Automated QC: check for variable type of GEOID
#               Common error: GEOID is read in as integer/numeric and the leading 0 gets dropped
#               for GEOIDs with state FIPS < 10
#
exp_block <- as.data.frame(exp_block)
GEOID <- names(exp_block)[grep("^GEOID|BLKID", names(exp_block))]

# Grab PM2.5 variable, which will change by year
#
exp_var <- names(exp_block)[grep(paste0("^", exp), names(exp_block))]

if (is(exp_block[[GEOID]])[1] != "character") { print("ERROR: wrong variable type for GEOID") 
} else { print(":) GEOID is correct type") }

# Create census ID variables for block group, tract, and county by 
# pulling out the first 12, 11, and 5 characters, respectively
#
exp_block$GEOID_BG <- substr(exp_block[[GEOID]], 1, 12)
exp_block$GEOID_Tract <- substr(exp_block[[GEOID]], 1, 11)
exp_block$GEOID_County <- substr(exp_block[[GEOID]], 1, 5)

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%%% CALCULATE POPULATION WEIGHTED MEANS BY CENSUS GEOGRAPHY %%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #

for (j in 1:length(censusgeos)) {
  cat("-----------------------------------------------------------\n")
  cat("Processing", censusgeos[j], "\n")
  
  # Calculate total population by geography (denominator of spatial weight)
  #
  pop_subgeo <- summaryBy(as.formula(paste0(pop_var_name, " ~ ", subgeoid[j])),
                          data = exp_block, FUN = sumfun)
  subgeopopvar <- names(pop_subgeo)[grep("^Pop", names(pop_subgeo))]
  
  # Merge these block group populations with the full block dataset
  #
  exp_block_subgeo <- merge(exp_block, pop_subgeo, by = c(subgeoid[j]), all.x = TRUE)
  
  # Calculate weight for blocks within block groups
  #
  subgeowt <- paste0(censusgeos[j], "Wt")
  exp_block_subgeo[[subgeowt]] <- exp_block_subgeo[[pop_var_name]] / exp_block_subgeo[[subgeopopvar]]
  
  # Calculate available weights, which accounts for missingness in eventual output
  #
  avail <- summaryBy(as.formula(paste0(subgeowt, " ~ ", subgeoid[j])),
                     data = exp_block_subgeo[which( !(is.na(exp_block_subgeo[[exp_var]])) ),],
                     FUN = sumfun)
  
  # Merge available weight with raw weight
  #
  exp_block_subgeo <- merge(exp_block_subgeo, avail, by = c(subgeoid[j]), all.x = TRUE)
  
  # Mark as NA any block-group-day/tract-day/county-day in which <50% of data are available
  #
  availwt <- paste0(subgeowt, ".sumfun")
  exp_block_subgeo[[availwt]][which(exp_block_subgeo[[availwt]] < 0.50)] <- NA
  
  # Adjust weight based on availability of data
  #
  exp_block_subgeo[[subgeowt]] <- exp_block_subgeo[[subgeowt]] / exp_block_subgeo[[paste0(subgeowt, ".sumfun")]]
  
  varwt <- paste0(exp_var, "Wt")
  
  exp_block_subgeo[[varwt]] <- exp_block_subgeo[[exp_var]] * exp_block_subgeo[[subgeowt]]
  
  # Automated QC -- Check to see if the weights all summed correctly
  #
  check1 <- summaryBy(as.formula(paste0(subgeowt, " ~ ", subgeoid[j])),
                      data = exp_block_subgeo[which( !(is.na(exp_block_subgeo[[exp_var]])) ),],
                      FUN = sumfun)
  
  # KRS: the above QC df (check1) isn't used; added below
  #
  if (length(which(round(check1[[paste0(subgeowt, ".sumfun")]], 4) != 1)) > 0) {
    cat("ERROR: weights do not sum to 1 for all geographies \n") } else { cat(":) weights sum to 1 for all geographies \n") }
  
  # Sum the partial weights of the variable to get the final population-weighted value
  #
  final_wt <- summaryBy(as.formula(paste0(varwt, " ~ ", subgeoid[j])),
                        data = exp_block_subgeo, FUN = sumfun)
  
  names(final_wt)[grep(exp, names(final_wt))] <- exp_var
  
  # Rename the "final" df to indicate which census geography it is
  #
  assign(paste0("final_", censusgeos[j]), final_wt)
}

# Save output data. Adjust as applicable (eg: if not using BG/Tract/County subdirectories)
#
saveRDS(final_BG, paste0(outdir, "BG_Weighted_10/", exp, "_PopWt_tl10_", year, "_", stateFIPS, "_BlockGroup.Rds"))
saveRDS(final_Tract, paste0(outdir, "Tract_Weighted_10/", exp, "_PopWt_tl10_", year, "_", stateFIPS, "_Tract.Rds"))
saveRDS(final_County, paste0(outdir, "County_Weighted_10/", exp, "_PopWt_tl10_", year, "_", stateFIPS, "_County.Rds"))
