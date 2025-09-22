# AMS 598 Project 1: Integer Count MapReduce

A MapReduce implementation for counting integers in the range 0-100 from multiple data files using SLURM job arrays.

## Usage

### Quick Start
```bash
# Make the master script executable
chmod +x submit_all.sh

# Submit both mapper and reducer jobs with proper dependencies
./submit_all.sh
```

### Manual Submission
```bash
# Submit mapper jobs first
MAPPER_JOB_ID=$(sbatch --parsable submit_mappers.slurm)

# Submit reducer with dependency on all mappers
sbatch --dependency=afterok:${MAPPER_JOB_ID} submit_reducer.slurm
```

### Monitoring Jobs
```bash
# Check job status
squeue -u $USER

# Check specific jobs
squeue -j <JOB_ID>

# View output files
ls -la /gpfs/projects/AMS598/class2025/YourNetID/slurm_output/
```

## Configuration

**Important**: Update the `NETID` variable in all SLURM scripts:
- `submit_mappers.slurm`
- `submit_reducer.slurm` 
- `submit_all.sh`

Change `NETID="SMARUPUDI"` to your actual NetID.

## How It Works

1. **Mapper Phase**: 4 parallel processes (array job 0-3) each process one data file
   - Maps array index to file: 0→data1, 1→data2, 2→data3, 3→data4
   - Counts integers 0-100 in assigned file
   - Saves results to intermediate files

2. **Reducer Phase**: Single process that waits for all mappers
   - Aggregates all mapper outputs
   - Reports top 6 integers by frequency
   - Archives intermediate files for verification

## File Processing

- Processes files: `project1_data_1.txt` through `project1_data_4.txt`
- Output files: `/gpfs/projects/AMS598/class2025/YourNetID/intermediate_files/mapper_output_X.txt`
- Final results: Printed to reducer output file

## Error Handling

- Validates input files exist before processing
- Handles malformed data gracefully
- Archives intermediate files instead of deleting for debugging
- Comprehensive logging in SLURM output files
