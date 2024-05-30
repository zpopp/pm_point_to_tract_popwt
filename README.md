# Block and Tract-Level PM2.5 Aggregation

# Project Overview
This repository includes code for the summation of PM2.5 component measures (EC, NH4, NO3, OC, SO4) and aggregation across the urban and non-urban datasets available through SEDAC. The PM2.5 data is shared as point data. To aggregate to the block-level, an intersection is run to merge blocks to the PM2.5 data which overlaps with each block. In instances where no points intersect a block, the nearest point is assigned as the block measure. After block-level aggregation, population weighting is used to develop measures for block group, tract, and county PM2.5 exposure, weighted for the distribution of PM2.5 across the block population within each of these larger geographies. A script is also provided to merge multiple states' data into a single nationwide tract file, as is available on the Harvard Dataverse CAFE collection.

State-level bash scripting was used to produce the final data product on Dataverse. Bash scripting is a way to substantially reduce processing time, but is only feasible with access to a computing cluster. The scripts include directions for processing a single state and avoiding the bash script. 

## Script Components
Three series of scripts are provided in this repository. The code/ directory includes scripts for:
- Single-Year Processing (single_year_process): Processing provided only for 2019 and using 2010 census geographies.
- Multi-Year Processing (multi_year_process): For processing all available years of PM2.5 data from 2000 to 2019 onto a single year's census geographies (2010 geographies in the provided code).

## DC Tutorial
The dc_tutorial/ directory provides data as well as scripts to support smaller scale processing of a single year for Washington, D.C. alone. The dc_tutorial/dc_rawdata/ directory includes raw PM2.5 point data that has been subset to the extent of the D.C. area. This smaller set of data was cut to allow for users to walk through the processing steps without downloading the larger data from SEDAC described in the depth below. The dc_tutorial/dc_code directory is very similar to the single-year-processing code, but without the bash scripts and other requirements for the larger scale processing.

# Usage
The PM2.5 point data in total nationally represents ~20GB of files across multiple components and urban/non-urban areas. Use of cloud storage is suggested for storing these data. 

# Data Sources
**Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban and 1km Non-Urban Area Grids for Contiguous U.S., v1 (2000 – 2019)**: 	[SEDAC Link to Raw Data](https://sedac.ciesin.columbia.edu/data/set/aqdh-pm2-5-component-ec-nh4-no3-oc-so4-50m-1km-contiguous-us-2000-2019)

Amini, H., M. Danesh-Yazdi, Q. Di, W. Requia, Y. Wei, Y. AbuAwad, L. Shi, M. Franklin, C.-M. Kang, J. M. Wolfson, P. James, R. Habre, Q. Zhu, J. S. Apte, Z. J. Andersen, X. Xing, C. Hultquist, I. Kloog, F. Dominici, P. Koutrakis, and J. Schwartz. 2023. Annual Mean PM2.5 Components (EC, NH4, NO3, OC, SO4) 50m Urban and 1km Non-Urban Area Grids for Contiguous U.S., 2000-2019 v1. Palisades, New York: NASA Socioeconomic Data and Applications Center (SEDAC). https://doi.org/10.7927/7wj3-en73. Accessed 10 October 2023.
	
# Workflow
## Single Year
The R code provided for single-year processing includes the following steps:
1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow for future joins of PM2.5 measures, running as bash by year (PM25TotalComponents.R)
2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks, running as bash by state due to large processing time (PM25_Process_Blocks.R)
3) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
4) Combine state-level tract shapefiles into nationwide tract dataset (Nationwide_Join.R)

## Multi-Year
The R code provided for multi-year processing includes the following steps:
1) Process raw annual PM2.5 components files from SEDAC into total_pm25 nationwide file, and add uid to allow  for future joins of PM2.5 measures, running as bash by year (01a_PM25TotalComponents.R)
2) Spatially join total components PM2.5 for one year (eg: 2019) to 2010 US census blocks, running as bash by state due to large processing time (02a_PM25_Process_Blocks.R)
3) Combine 2000 to 2018 years of PM2.5 data by UID to allow for merger with 2019 (03_PM25_Join_2000to2018.R; this file)
4) Use UID-block combination to join pm2.5 for other years to the census blocks without re-running the spatial processing (04_PM25_Join_AllYrs.R)
5) Aggregate data from block to block group/tract/county based on population or area (PopWt*.R)
6) Combine state-level shapefiles into nationwide tract dataset. (Combine_Nationwide_Files.R)


# Dependencies
The processing in this code to produce the Harvard Dataverse file was done using bash scripting. 

The following R packages are used: sf, dplyr, plyr, data.table, tigris, doBy, tidyverse, tidycensus

# Contact Information: 
For correspondence about this processing, contact Zach Popp (zpopp@bu.edu)
