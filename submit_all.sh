#!/bin/bash

NETID="SMARUPUDI"  
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"

# Create necessary directories
mkdir -p "${BASE_DIR}/slurm_output"
mkdir -p "${BASE_DIR}/intermediate_files"

# Submit mapper jobs
MAPPER_JOB_ID=$(sbatch --parsable submit_mappers.slurm)
echo "Mapper jobs submitted: ${MAPPER_JOB_ID}"

# Submit reducer job with dependency on mapper jobs
REDUCER_JOB_ID=$(sbatch --parsable --dependency=afterok:${MAPPER_JOB_ID} submit_reducer.slurm)
echo "Reducer job submitted: ${REDUCER_JOB_ID}"

echo "Use 'squeue -u \$USER' to monitor your jobs"