#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=00:10:00
#SBATCH --partition=short-40core
#SBATCH --job-name=Integer_Reducer
#SBATCH --output=reducer_%j.out
#SBATCH --error=reducer_%j.err
#SBATCH --open-mode=append

module load anaconda/3

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

