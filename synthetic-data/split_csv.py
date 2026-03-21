import csv
import sys
import os


def split_csv(input_path, num_files=4):
    # Read all rows from the CSV
    with open(input_path, newline='', encoding='utf-8') as csvfile:
        reader = list(csv.reader(csvfile))
        header = reader[0]
        data_rows = reader[1:]

    total_rows = len(data_rows)
    chunk_size = total_rows // num_files
    remainder = total_rows % num_files

    # Calculate start/end indices for each chunk
    indices = []
    start = 0
    for i in range(num_files):
        end = start + chunk_size + (1 if i < remainder else 0)
        indices.append((start, end))
        start = end

    import datetime
    base, ext = os.path.splitext(os.path.basename(input_path))
    out_dir = os.path.dirname(input_path)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, (start, end) in enumerate(indices):
        out_path = os.path.join(out_dir, f"{base}_part{i+1}_{timestamp}{ext}")
        with open(out_path, 'w', newline='', encoding='utf-8') as out_csv:
            writer = csv.writer(out_csv)
            writer.writerow(header)
            writer.writerows(data_rows[start:end])
        print(f"Wrote {end-start} rows to {out_path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Split a CSV file into multiple smaller files.")
    parser.add_argument("input_csv", help="Path to the input CSV file.")
    parser.add_argument("-n", "--num_files", type=int, default=4, help="Number of output files (default: 4)")
    args = parser.parse_args()

    input_path = args.input_csv
    num_files = args.num_files
    if not os.path.isfile(input_path):
        print(f"File not found: {input_path}")
        sys.exit(1)
    if num_files < 1:
        print("Number of files must be at least 1.")
        sys.exit(1)
    split_csv(input_path, num_files)


if __name__ == "__main__":
    main()
