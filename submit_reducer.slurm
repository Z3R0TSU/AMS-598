#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=00:10:00
#SBATCH --partition=short-40core
#SBATCH --job-name=Integer_Reducer
#SBATCH --output=/gpfs/projects/AMS598/class2025/SMARUPUDI/slurm_output/reducer_%j.out
#SBATCH -e stderr.txt
#SBATCH -o stdout.txt
#SBATCH --open-mode=append

module load anaconda/3

BASE_DIR="/gpfs/home/smarupudi/AMS_598_Project1"
TMP_DIR="/gpfs/projects/AMS598/class2025/Marupudi_SrinivasaPhaniMadhav"

python integers_count.py reducer /gpfs/projects/AMS598/class2025/Marupudi_SrinivasaPhaniMadhav
