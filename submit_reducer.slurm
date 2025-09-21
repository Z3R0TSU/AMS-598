#!/bin/bash
#SBATCH --job-name=Integer_Count_Reducer
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:10:00
#SBATCH --partition=short-40core
#SBATCH --output=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/reducer_%j.out
#SBATCH --error=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/reducer_%j.err

# Load required modules
module load anaconda/3

# Set up directories and variables
NETID="SMARUPUDI"
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"
TMP_DIR="${BASE_DIR}/intermediate_files"

# Echo job information
echo "=================================================="
echo "REDUCER JOB STARTING"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node: $(hostname)"
echo "Date: $(date)"
echo "=================================================="

# Verify all mapper output files exist before proceeding
echo "Checking for mapper output files..."
for i in {0..3}; do
    mapper_file="${TMP_DIR}/mapper_output_${i}.txt"
    if [ ! -f "$mapper_file" ]; then
        echo "ERROR: Missing mapper output file: $mapper_file"
        exit 1
    else
        echo "Found: $mapper_file"
    fi
done

echo "All mapper output files found. Starting reducer..."

# Run the reducer
python integers_count.py "reducer" "${TMP_DIR}"

echo "Reducer completed at $(date)"

# Optional: Archive intermediate files instead of deleting them
# This is useful for debugging and verification
ARCHIVE_DIR="${BASE_DIR}/archive/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${ARCHIVE_DIR}"
echo "Archiving intermediate files to ${ARCHIVE_DIR}"
cp "${TMP_DIR}"/mapper_output_*.txt "${ARCHIVE_DIR}/" 2>/dev/null || echo "No mapper output files to archive"