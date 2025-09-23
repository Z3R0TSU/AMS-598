import os
import sys
from collections import defaultdict
from multiprocessing import Pool


def mapper_worker(args):
    input_file, tmp_dir = args
    file_name = os.path.basename(input_file)
    output_file = os.path.join(tmp_dir, f"mapper_output_{file_name}.txt")
    counts = defaultdict(int)
    try:
        with open(input_file, "r") as f:
            for line in f:
                for num in line.strip().split():
                    if num.isdigit():
                        val = int(num)
                        if 0 <= val <= 100:
                            counts[val] += 1
    except Exception as e:
        print(f"Error reading {input_file}: {e}", file=sys.stderr)
        return
    try:
        with open(output_file, "w") as f:
            for num, count in counts.items():
                f.write(f"{num}\t{count}\n")
        print(f"Mapper finished {input_file} → {output_file}")
    except Exception as e:
        print(f"Error writing {output_file}: {e}", file=sys.stderr)


def mapper(tmp_dir, data_dir):
    os.makedirs(tmp_dir, exist_ok=True)
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    with Pool(processes=4) as pool:
        pool.map(mapper_worker, [(f, tmp_dir) for f in files])


def reducer(tmp_dir, output_file="reduce_result.txt"):
    total_counts = defaultdict(int)
    for file_name in os.listdir(tmp_dir):
        if file_name.startswith("mapper_output_"):
            file_path = os.path.join(tmp_dir, file_name)
            try:
                with open(file_path, "r") as f:
                    for line in f:
                        num, count = line.strip().split("\t")
                        total_counts[int(num)] += int(count)
            except Exception as e:
                print(f"Error reading {file_path}: {e}", file=sys.stderr)
    top_six = sorted(total_counts.items(), key=lambda x: x[1], reverse=True)[:6]
    output_path = os.path.join(tmp_dir, output_file)
    try:
        with open(output_path, "w") as f:
            for num, count in top_six:
                f.write(f"{num}\t{count}\n")
        print(f"Reducer finished. Top 6 results saved to {output_file}")
    except Exception as e:
        print(f"Error writing {output_file}: {e}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)
    mode = sys.argv[1]
    if mode == "mapper":
        if len(sys.argv) != 4:
            sys.exit(1)
        tmp_dir = sys.argv[2]
        data_dir = sys.argv[3]
        mapper(tmp_dir, data_dir)
    elif mode == "reducer":
        if len(sys.argv) != 3:
            sys.exit(1)
        tmp_dir = sys.argv[2]
        reducer(tmp_dir)
    else:
        sys.exit(1)

