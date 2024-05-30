#!/bin/bash -l

#$ -N pm25_block_10  ## Name the job
#$ -j y         ## Merge error & output files
#$ -pe omp 16
#$ -l mem_per_core=8G  ## Request between 16 GB and 32 GB of RAM (may require more - see below)

module load R/4.2.1
Rscript code_dir/02a_PM25_Process_Blocks.R $SGE_TASK_ID

## In Terminal, cd to the directory in which this bash script is located. 
## qsub -P project -t 1-49 02b_PM25_Process_Blocks.sh
##
## This bash script will run an array of jobs for each index 1 to 49. This index
## will be read into the R scripts to specify specific U.S. States. Alternatively,
## the processing could be run for each state.
##
## The specifics of this bash script may vary based on your computing environment