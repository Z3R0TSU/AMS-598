import os
import sys
from collections import defaultdict

def mapper(file_id, tmp_dir, data_dir):
    """
    Counts integers (0-100) in a specific file and saves the results.
    """
    file_number = int(file_id) + 1
    # File naming for HPC environment
    input_file = os.path.join(data_dir, f"project1_data_{file_number}.txt")
    output_file = os.path.join(tmp_dir, f"mapper_output_{file_id}.txt")

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.", file=sys.stderr)
        return

    counts = defaultdict(int)

    # Read the file and count integers
    try:
        with open(input_file, 'r') as f:
            for line in f:
                for num in line.split():
                    if num.isdigit():
                        num = int(num)
                        if 0 <= num <= 100:
                            counts[num] += 1
    except Exception as e:
        print(f"Error reading {input_file}: {e}", file=sys.stderr)
        return

    # Write the counts to the output file
    with open(output_file, 'w') as f:
        for num, count in counts.items():
            f.write(f"{num}\t{count}\n")

    print(f"Mapper {file_id} finished. Results saved to {output_file}")

def reducer(tmp_dir):
    """
    Combines counts from all mapper output files and shows the top 6 integers.
    """
    total_counts = defaultdict(int)

    # Read all mapper output files
    for file_name in os.listdir(tmp_dir):
        if file_name.startswith("mapper_output_"):
            file_path = os.path.join(tmp_dir, file_name)
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        num, count = line.split('\t')
                        total_counts[int(num)] += int(count)
            except Exception as e:
                print(f"Error reading {file_path}: {e}", file=sys.stderr)

    # Find the top 6 integers
    top_six = sorted(total_counts.items(), key=lambda x: x[1], reverse=True)[:6]

    print("Top 6 integers:")
    for num, count in top_six:
        print(f"{num}: {count}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python integers_count.py <mode> [args...]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "mapper":
        if len(sys.argv) != 5:
            print("Usage: python integers_count.py mapper <file_id> <tmp_dir> <data_dir>")
            sys.exit(1)
        mapper(sys.argv[2], sys.argv[3], sys.argv[4])

    elif mode == "reducer":
        if len(sys.argv) != 3:
            print("Usage: python integers_count.py reducer <tmp_dir>")
            sys.exit(1)
        reducer(sys.argv[2])

    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)