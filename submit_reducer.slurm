#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=00:10:00
#SBATCH --partition=short-40core
#SBATCH --job-name=Integer_Reducer
# Note: When submitting, use --dependency=afterok:MAPPER_JOB_ID
#SBATCH --output=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/reducer_%j.out
#SBATCH -e stderr.txt
#SBATCH -o stdout.txt
#SBATCH --open-mode=append

# Load required modules
module load anaconda/3

# IMPORTANT: Replace NETID with your own NetID before submitting
NETID="SMARUPUDI" 
BASE_DIR="/gpfs/projects/AMS598/class2025/${NETID}"
TMP_DIR="${BASE_DIR}/intermediate_files"

for i in {0..3}; do
    if [ ! -f "${TMP_DIR}/mapper_output_${i}.txt" ]; then
        echo "Error: Missing mapper output file ${i}"
        exit 1
    fi
done

python integers_count.py reducer "${TMP_DIR}"

