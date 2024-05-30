#!/bin/bash -l

#$ -N pm_join_10 ## Name the job
#$ -j y         ## Merge error & output files
#$ -pe omp 16

module load R/4.3.1
Rscript code_dir/04a_PM25_Join_AllYrs.R $SGE_TASK_ID

## In Terminal, cd to the directory in which this bash script is located. 
## qsub -P project -t 1-49 04b_PM25_Join_AllYrs.sh
##
## This bash script will run an array of jobs for each index 1 to 49. This index
## will be read into the R scripts to specify specific U.S. States. Alternatively,
## the processing could be run for each state.
##
## The specifics of this bash script may vary based on your computing environment