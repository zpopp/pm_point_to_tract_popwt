#!/bin/bash -l

#$ -N pm25_popwt  ## Name the job
#$ -j y         ## Merge error & output files
#$ -pe omp 16

module load R/4.2.1
Rscript code_dir/05a_PopWt_AnyExp_Longitudinal_PM25_BgTrCo_v01.R $SGE_TASK_ID

## In Terminal, cd to the directory in which this bash script is located. 
## qsub -P project -t 1-49 05b_PopWt_AnyExp_Longitudinal_PM25_BgTrCo_v01.sh
##
## This bash script will run an array of jobs for each index 1 to 49. This index
## will be read into the R scripts to specify specific U.S. States. Alternatively,
## the processing could be run for each state.
##
## The specifics of this bash script may vary based on your computing environment