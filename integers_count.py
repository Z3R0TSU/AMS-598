import os
import sys
from collections import defaultdict

def mapper(file_id, tmp_dir, data_dir):

    file_path = os.path.join(data_dir, f"project1_data_{file_id}.txt")
    output_path = os.path.join(tmp_dir, f"mapper_output_{file_id}.txt")

    counts = defaultdict(int)
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}", file=sys.stderr)
        return

    try:
        with open(file_path, 'r') as f:
            for line in f:
                integers = line.strip().split()
                for num_str in integers:
                    try:
                        num = int(num_str)
                        if 0 <= num <= 100:
                            counts[num] += 1
                    except ValueError:
                        # Skip lines that are not valid integers
                        continue
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return

    with open(output_path, 'w') as out_f:
        for number, count in counts.items():
            out_f.write(f"{number}\t{count}\n")
    
    print(f"Mapper {file_id} completed. Results saved to {output_path}")

def reducer(tmp_dir):
    """
    Reads all temporary files, aggregates the counts, and reports the top 6.
    """
    all_counts = defaultdict(int)
    

    for filename in os.listdir(tmp_dir):
        if filename.startswith("mapper_output_"):
            file_path = os.path.join(tmp_dir, filename)
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        try:
                            number_str, count_str = line.strip().split('\t')
                            number = int(number_str)
                            count = int(count_str)
                            all_counts[number] += count
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                print(f"Error reading intermediate file {file_path}: {e}", file=sys.stderr)
                continue

    sorted_counts = sorted(all_counts.items(), key=lambda item: item[1], reverse=True)

    print("--- Final Results ---")
    print("Top 6 integers with the highest frequencies:")
    for number, count in sorted_counts[:6]:
        print(f"Integer: {number}, Frequency: {count}")
    
    # Optional: Clean up intermediate files
    # for filename in os.listdir(tmp_dir):
    #    if filename.startswith("mapper_output_"):  
    #        os.remove(os.path.join(tmp_dir, filename))
    # os.rmdir(tmp_dir) # Only if directory is empty


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python count_integers.py <mode> [args...]")
        sys.exit(1)

    mode = sys.argv[1]
    
    if mode == "mapper":
        if len(sys.argv) != 5:
            print("Usage: python count_integers.py mapper <file_id> <tmp_dir> <data_dir>")
            sys.exit(1)
        file_id = sys.argv[2]
        tmp_dir = sys.argv[3]
        data_dir = sys.argv[4]
        mapper(file_id, tmp_dir, data_dir)
        
    elif mode == "reducer":
        if len(sys.argv) != 3:
            print("Usage: python count_integers.py reducer <tmp_dir>")
            sys.exit(1)
        tmp_dir = sys.argv[2]
        reducer(tmp_dir)
        
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)