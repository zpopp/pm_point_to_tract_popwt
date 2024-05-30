#        Author:  Zach Popp 
#  Date created:  2023-09-14 (v01)
# Last modified:  2023-01-12 (v02)
#  Version Hist:  NA
#   Modified by:  adapted from Keith S.
#       Purpose:  Take block-level estimates (by state and year), and aggregate
#                 using population weights up to the block group, census tract,
#                 and county levels. The weighting is derived from template code
#                 that had been created for PRISM block values, and has been modified
#                 to run for all variables across years here based on parameters
#                 input at the beginning of the script.
##Overall Processing Steps:
## 1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow 
##    for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
## 2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks,
##    running as bash by state due to large processing time (02a_PM25_Process_Blocks.R)
## 3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R)
## 4) Use UID-block combination to join pm2.5 for other years to the census blocks
##    without re-running the spatial processing (04_PM25_Join_AllYrs.R)
## 5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R; this file)
## 6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R)

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
# that year. This script is for longitudinal data.
#
# Also, the area weighting code differs for daily v. annual measures. This 
# code is relevant for annual data.

library("terra")  # For raster data
library("sf")     # For vector data
library("plyr")
library("tigris") # For downloading census shapefiles
library("doBy")
library("tidyverse")
library("tidycensus")
library("lwgeom")
sf_use_s2(FALSE) # S2 is for computing distances, areas, etc. on a SPHERE (using
# geographic coordinates, i.e., decimal-degrees); no need for this
# extra computational processing time if using PROJECTED coordinates,
# since these are already mapped to a flat surface. Here, PM2.5
# is in geographic coordinates, but the scale of areas we are 
# interested in is very small, and hence the error introduced by 
# ignoring the Earth's curvature over these tiny areas is negligible and
# a reasonable trade off given the dramatic reduction in processing time

# The following variables come from the command line when running as bash
# The inputs should be 1-49 to run for CONUS states
#
args <- commandArgs(trailingOnly = TRUE)
b <- as.numeric(args[1]) 

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
fipsdir <- ""  # directory where state FIPS code list is stored (only required if
               # using bash script)
states <- read.csv(paste0(fipsdir, "US_States_FIPS_Codes.csv"), stringsAsFactors = FALSE)
states <- states %>% filter(StAbbrev != "AK" & StAbbrev != "HI")
states$StFIPS <- formatC(as.numeric(states$StFIPS), width = 2, format = "d", flag = "0")
state_mapping <- states[, c("StFIPS", "StAbbrev")]  # Keep relevant columns
stateFIPS <- states$StFIPS
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")
state_abbrev <- state_mapping$StAbbrev[state_mapping$StFIPS == stateFIPS]

# The exp object is meant to be the character string that describes the exposure data
# which you are seeking to population weight. Later in the script, the
# all variables beginning with this prefix will be queried for weighting.
#
exp <- "pm25"
  
# The year object represents the year for which the data were queried.
#
year <- 2010    

# The decennial year will be estimated based on the year input. This will be used
# to query the necessary block population data for weighting, which is only 
# available for decennial census years.
#
decyear <- ifelse(year >= 2020, 2020,
                      ifelse(year %in% c(2010:2019), 2010,
                             ifelse(year %in% c(2000:2009), 2000, NA)))


# The deyr variable is a substring 2-digit representation of the decennial year
# for reading in and outputting files
deyr <- substr(decyear, 3,4)

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

# The shpdir will be where the census geographies and GEOIDs for the relevant decennial
# census year are stored. If you are using non decennial census data, the decyear 
# object in the statement below can be modified to the year object. This only
# will read in the block shapefile if the data is already stored.
#
# if not downloaded, see pm_point_to_tract_popwt/code/02_PM25_Process_Blocks_10.R
# for code to download from tigris
#
shpdir <- "" 

# The exposure_file is the file containing the block-level measures. The paste0
# function will string together the bash input and decennial year information
# to query the appropriate file
# This should be stored in the pm_outdir from the 04a_PM25_Join_AllYrs.R script
#
pm_outdir <- ""

exposure_file <- paste0(pm_outdir, "tl", deyr, "_bl_PM2.5_allyr_",stateFIPS,".rds")

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
counties <- tigris::counties(state = stateFIPS, year = decyear, cb = TRUE)
counties <- unique(counties[[grep("^COUNTYFP", names(counties), ignore.case = TRUE)]])

# Total population variable will vary by Decennial Census
#
variable <- ifelse(decyear %in% 2000:2009, "PL001001",
                   ifelse(decyear %in% 2010:2019, "P001001",
                          ifelse(decyear %in% 2020:2029, "P1_001N", NA)))

blockpop <- get_decennial(geography = "block",
                          variables = variable, # Use load_variables("pl", year = 2020) to see available vars
                          year = decyear,
                          state = stateFIPS,
                          county = counties,
                          sumfile = "pl")

# Automated QC check -- confirm data is total population
#
if (length(which(blockpop[[variable]] != variable)) > 0) {
  cat("ERROR: Not all pop. vars. \n") } else { cat(":) all pop. vars. \n") }

# Rename population variable
#
pop_var_name <- paste0("Pop_", decyear)
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
GEOID <- names(exp_block)[grep("^GEOID|BLKID", names(exp_block))]

# Convert exposure data to data.frame object from SpatVector (otherwise GEOID var reads as data.frame)
#
exp_block <- as.data.frame(exp_block)

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

# Grab exposure variable, which will change by year
#
varnames <- names(exp_block)[grep(paste0("^", exp), names(exp_block))]

for (j in 1:length(censusgeos)) {
  
  cat("-----------------------------------------------------------\n")
  cat("Processing", censusgeos[j], "\n")
  
  for (i in 1:length(varnames)) {
    
    cat("-----------------------------------------------------------\n")
    cat("Processing", varnames[i], "\n")
    
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
                       data = exp_block_subgeo[which( !(is.na(exp_block_subgeo[[varnames[i]]])) ),],
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
    
    varwt <- paste0(varnames[i], "Wt")
    
    exp_block_subgeo[[varwt]] <- exp_block_subgeo[[varnames[i]]] * exp_block_subgeo[[subgeowt]]
    
    # Automated QC -- Check to see if the weights all summed correctly
    #
    check1 <- summaryBy(as.formula(paste0(subgeowt, " ~ ", subgeoid[j])),
                        data = exp_block_subgeo[which( !(is.na(exp_block_subgeo[[varnames[i]]])) ),],
                        FUN = sumfun)
    
    # Sum the partial weights of the variable to get the final population-weighted value
    #
    final_wt <- summaryBy(as.formula(paste0(varwt, " ~ ", subgeoid[j])),
                          data = exp_block_subgeo, FUN = sumfun)
    
    names(final_wt)[grep(exp, names(final_wt))] <- varnames[i]
    
    if (varnames[i] == varnames[1]) {
      final <- final_wt
    } else {
      final <- merge(final, final_wt, by = subgeoid[j])
    }
    
  }
  
  # Rename the "final" df to indicate which census geography it is
  #
  assign(paste0("final_", censusgeos[j]), final)
}

# Save output data
#
saveRDS(final_BG, paste0(outdir, "BlockGroup_Wt_10/", exp, "_PopWt_", decyear, "_", stateFIPS, "_BlockGroup_AllYrs.Rds"))
saveRDS(final_Tract, paste0(outdir, "Tract_Wt_10/", exp, "_PopWt_", decyear, "_",  stateFIPS, "_Tract_AllYrs.Rds"))
saveRDS(final_County, paste0(outdir, "County_Wt_10/", exp, "_PopWt_", decyear, "_",  stateFIPS, "_County_AllYrs.Rds"))

