# ********************************************************************** #
# ~~~~~~~~~~ POPULATION WEIGHTED MEAN - PM2.5 2010  ~~~~~~~~~~~~~~~~~~~~ #
# ********************************************************************** #
#  Date created:  2023-09-14 (v01)
# Last modified:  2023-01-12 (v02)
#   Modified by:  Zach P. (zpopp@bu.edu; adapted from Keith S.)
#       Purpose:  Take block-level estimates (by state and year), and aggregate
#                 using population weights up to the block group, census tract,
#                 and county levels. The script has been developed to make the 
#                 use of different exposure sources at the block level more 
#                 easy without requiring extensive adjustments 

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%% OVERVIEW  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% #
#
# This script has been created to process exposure data
# from the block-level up to larger geographies which blocks nest within. Instead of 
# copying, pasting, and re-adjusting scripts, novel exposure data can be processed
# based on adjustment of a handful of inputs within the STEP 1. ESTABLISH DATA INPUTS
# section below. Please read the section carefully before proceeding.
#
# This script is for data which will be joined to census data for the 
# end of the decade in which the data was collected (year-by-year).
#
# Also, the population weighting process differs for daily v. annual measures. This 
# code is relevant for annual data.
#

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

# The following variables come from the command line when running as bash. We 
# have commented out the bash lines and added inputs relevant to the tutorial.
# For more information about bash scripts, see Base Scripting tutorial (link)
#
#args <- commandArgs(trailingOnly = TRUE)
#b <- as.numeric(args[1]) # county FIPS index; b = 290 for DC in 2000 and 291 for 2019/2020 # KRS: I don't think this works with your code since you stratify by state not county. I think DC should be 8
#year <- as.numeric(args[2])

b <- 8 # test state 
year <- 2010

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
states_in <- states(year = 2010)
states_in <- states_in %>% filter(STUSPS10 != "AK" & STUSPS10 != "HI") %>%
  arrange(GEOID10)
stateFIPS <- states_in$STATEFP10     #integer; does not contain leading zeroes
stateFIPS <- formatC(stateFIPS[b], width = 2, format = "fg", flag = "0")

state_abbrev <- states_in$STUSPS10[states_in$STATEFP10 == stateFIPS]

# The exp object is meant to be the character string that describes the exposure data
# which you are seeking to population weight. Later in the script, the
# all variables beginning with this prefix will be queried for weighting.
#
exp <- "pm25"

# Depending on the goal of your output, two 'decennial' inputs are needed. One is for the 
# year of census geographies that will be used (decyear_geo), which will be the
# end of the decade in which the data was collected to represent all changes that
# have been made over the course of the decade. The second is the decennial census 
# for the beginning of the year, which will be used to query the relevant block
# population (decyear_pop). For this process, we will use 2010 for both, but in
# some cases, the user may which to use the end of the decade for the geographic
# year as the end of the decade will include all changes in geography and naming
# that occur over the course of the decade.
#
decyear_geo <- 2010
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
outdir <- "dc_tutorial/dc_outputata"

# The exposure_file is the file containing the block-level measures. The paste0
# function will string together the bash input and decennial year information
# to query the appropriate file
#
exposure_file <- paste0(outdir, "tl10_bl_PM2.5_2010_mean_", stateFIPS, ".rds")

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
                          variables = variable, # Use load_variables("pl", year = 2020) to see available vars
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
# %%%%%%%% STEP 4: AGGREGATE UP FROM BLOCK TO BG, TRACT, AND COUNTY %%%%%%%%%% #
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

# Save output data (tabular)
#
saveRDS(final_BG, paste0(outdir, "tl10_bg_", exp, "_PopWt_", year, "_", stateFIPS, ".Rds"))
saveRDS(final_Tract, paste0(outdir, "tl10_tr_", exp, "_PopWt_", year, "_", stateFIPS, ".Rds"))
saveRDS(final_County, paste0(outdir, "tl10_co_", exp, "_PopWt_", year, "_", stateFIPS, ".Rds"))

# Rejoin with spatial data
# Read in spatial data for relevant units
#
BG_DC <- block_groups(state = "11", year = 2010)
Tract_DC <- tracts(state = "11", year = 2010)
County_DC <- counties(state = "11", year = 2010)

# Rename GEOIDs in PM output
#
final_BG_sf <- final_BG %>% dplyr::rename(GEOID10 = GEOID_BG)
final_Tract_sf <- final_Tract %>% dplyr::rename(GEOID10 = GEOID_Tract)
final_County_sf <- final_County %>% dplyr::rename(GEOID10 = GEOID_County)

# Join PM to spatial data
#
final_BG_sf <- left_join(BG_DC, final_BG_sf, by = "GEOID10")
final_Tract_sf <- left_join(tract_DC, final_Tract_sf, by = "GEOID10")
final_County_sf <- left_join(BG_DC, final_County_sf, by = "GEOID10")

# Save output data as gpkg
#
st_write(final_BG_sf, paste0(outdir, "tl10_bg_", exp, "_PopWt_", year, "_", stateFIPS, ".gpkg"))
st_write(final_Tract_sf, paste0(outdir, "tl10_tr_", exp, "_PopWt_", year, "_", stateFIPS, ".gpkg"))
st_write(final_County_sf, paste0(outdir, "tl10_co_", exp, "_PopWt_", year, "_", stateFIPS, ".gpkg"))

