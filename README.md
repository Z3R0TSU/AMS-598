# AMS 598 Project 1: Integer Count with MapReduce

This project implements a MapReduce solution to count integers (0-100) across multiple data files. It uses SLURM job arrays to run 4 mapper processes in parallel, followed by a single reducer that aggregates the results.

## What's in this repo

- `integers_count.py` - Main Python script with mapper and reducer functions
- `submit_mappers.slurm` - SLURM script to run 4 mapper jobs in parallel
- `submit_reducer.slurm` - SLURM script for the reducer (waits for mappers to finish)
- `submit_all.sh` - Convenience script to submit everything at once
- `data/` - Sample data files for local testing

## Quick Setup

1. **Update your NetID** in the SLURM scripts:
   ```bash
   # Change this line in all three files:
   NETID="SMARUPUDI"  # Replace with your actual NetID
   ```

2. **Submit the jobs**:
   ```bash
   chmod +x submit_all.sh
   ./submit_all.sh
   ```

That's it! The script handles dependencies automatically.

## How it works

### Mapper Phase (4 parallel jobs)
Each mapper processes one data file and counts integers 0-100:
- Mapper 0 → `project1_data_1.txt`
- Mapper 1 → `project1_data_2.txt`
- Mapper 2 → `project1_data_3.txt`
- Mapper 3 → `project1_data_4.txt`

Results are saved as tab-separated files in `/gpfs/projects/AMS598/class2025/YourNetID/intermediate_files/`

### Reducer Phase (1 job)
Waits for all mappers to complete, then:
- Reads all intermediate files
- Combines the counts
- Reports the top 6 most frequent integers

## Manual submission (if needed)

```bash
# Submit mappers first
MAPPER_JOB_ID=$(sbatch --parsable submit_mappers.slurm)

# Submit reducer with dependency
sbatch --dependency=afterok:${MAPPER_JOB_ID} submit_reducer.slurm
```

## Checking job status

```bash
# See all your jobs
squeue -u $USER

# Check specific job output
ls -la /gpfs/projects/AMS598/class2025/YourNetID/slurm_output/
```

## Notes

- The intermediate files are kept for debugging (not deleted automatically)
- Make sure to update your NetID before submitting!
- If jobs fail, check the `.out` and `.err` files in your slurm_output directory
