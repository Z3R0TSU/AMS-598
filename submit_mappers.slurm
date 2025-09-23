#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=00:15:00
#SBATCH --partition=short-40core
#SBATCH --job-name=Integer_Mapper
#SBATCH --output=/gpfs/projects/AMS598/class2025/Marupudi_SrinivasaPhaniMadhav/slurm_output/stdout.out
#SBATCH --error=/gpfs/projects/AMS598/class2025/Marupudi_SrinivasaPhaniMadhav/slurm_output/stderr.out
#SBATCH --open-mode=append


module load anaconda/3

python integers_count.py mapper /gpfs/projects/AMS598/class2025/Marupudi_SrinivasaPhaniMadhav /gpfs/projects/AMS598/projects2025_data/project1_data
