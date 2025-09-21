#!/bin/bash

# Master submission script for Integer Count MapReduce Job
# This script coordinates the submission of mapper and reducer jobs

echo "=================================================="
echo "INTEGER COUNT MAPREDUCE JOB SUBMISSION"
echo "Date: $(date)"
echo "=================================================="

# Set up variables
NETID="SMARUPUDI"
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"

# Create necessary directories
mkdir -p "${BASE_DIR}/slurm_output"
mkdir -p "${BASE_DIR}/intermediate_files"
mkdir -p "${BASE_DIR}/archive"

echo "Created necessary directories under ${BASE_DIR}"

# Submit the mapper jobs
echo "Submitting mapper jobs..."
MAPPER_JOB_ID=$(sbatch --parsable submit_mappers.slurm)

if [ $? -eq 0 ]; then
    echo "Mapper jobs submitted successfully with Job ID: ${MAPPER_JOB_ID}"
else
    echo "ERROR: Failed to submit mapper jobs"
    exit 1
fi

# Submit the reducer job with dependency on all mapper jobs
echo "Submitting reducer job with dependency on mapper jobs..."
REDUCER_JOB_ID=$(sbatch --parsable --dependency=afterok:${MAPPER_JOB_ID} submit_reducer.slurm)

if [ $? -eq 0 ]; then
    echo "Reducer job submitted successfully with Job ID: ${REDUCER_JOB_ID}"
    echo "Reducer will start after all mapper jobs complete successfully"
else
    echo "ERROR: Failed to submit reducer job"
    exit 1
fi

echo "=================================================="
echo "JOB SUBMISSION SUMMARY"
echo "Mapper Array Job ID: ${MAPPER_JOB_ID}"
echo "Reducer Job ID: ${REDUCER_JOB_ID}"
echo "=================================================="
echo ""
echo "To monitor your jobs:"
echo "  squeue -u \$USER"
echo "  squeue -j ${MAPPER_JOB_ID}"
echo "  squeue -j ${REDUCER_JOB_ID}"
echo ""
echo "To check job output:"
echo "  ls -la ${BASE_DIR}/slurm_output/"
echo ""
echo "To cancel jobs if needed:"
echo "  scancel ${MAPPER_JOB_ID}  # Cancel all mapper jobs"
echo "  scancel ${REDUCER_JOB_ID}  # Cancel reducer job"
echo ""
echo "Job submission completed!"