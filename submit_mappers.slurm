#!/bin/bash
#SBATCH --job-name=Integer_Count_Mappers
#SBATCH --array=0-3
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:15:00
#SBATCH --partition=short-40core
#SBATCH --output=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/mapper_%A_%a.out
#SBATCH --error=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/mapper_%A_%a.err

# Load required modules
module load anaconda/3

# Set up directories and variables
NETID="SMARUPUDI"
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"
TMP_DIR="${BASE_DIR}/intermediate_files"
DATA_DIR="/gpfs/projects/AMS598/projects2025_data/project1_data"

# Create necessary directories
mkdir -p "${TMP_DIR}"
mkdir -p "${BASE_DIR}/slurm_output"

# Echo job information
echo "=================================================="
echo "MAPPER JOB ${SLURM_ARRAY_TASK_ID} STARTING"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Array Job ID: ${SLURM_ARRAY_JOB_ID}"
echo "Task ID: ${SLURM_ARRAY_TASK_ID}"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "=================================================="

# Run the mapper
echo "Starting mapper for file with ID ${SLURM_ARRAY_TASK_ID}"
python integers_count.py "mapper" "${SLURM_ARRAY_TASK_ID}" "${TMP_DIR}" "${DATA_DIR}"

echo "Mapper ${SLURM_ARRAY_TASK_ID} completed at $(date)"