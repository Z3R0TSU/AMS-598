#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=00:15:00
#SBATCH --partition=short-40core
#SBATCH --job-name=Integer_Mapper
#SBATCH --array=0-3
#SBATCH --output=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/mapper_%A_%a.out
#SBATCH -e stderr.txt
#SBATCH -o stdout.txt
#SBATCH --open-mode=append

# Load required modules
module load anaconda/3

# IMPORTANT: Replace NETID with your own NetID before submitting
NETID="SMARUPUDI"  
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"
TMP_DIR="${BASE_DIR}/intermediate_files"
DATA_DIR="/gpfs/projects/AMS598/projects2025_data/project1_data"


mkdir -p "${TMP_DIR}"
mkdir -p "${BASE_DIR}/slurm_output"

python integers_count.py mapper "${SLURM_ARRAY_TASK_ID}" "${TMP_DIR}" "${DATA_DIR}"