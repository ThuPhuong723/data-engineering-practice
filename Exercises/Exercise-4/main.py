import json
import csv
import os
from pathlib import Path
import glob

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f"{name}{i}_")
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def process_json_file(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Could not decode {json_file}: {e}")
            return

    # flatten the json object
    flat_data = flatten_json(data)

    # Write to CSV with the same name
    csv_file = json_file.replace('.json', '.csv')
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvf:
        writer = csv.DictWriter(csvf, fieldnames=flat_data.keys())
        writer.writeheader()
        writer.writerow(flat_data)

    print(f"Converted {json_file} -> {csv_file}")

def main():
    # Recursively find all .json files in the data directory
    json_files = glob.glob('data/**/*.json', recursive=True)

    print(f"Found {len(json_files)} JSON files.")
    for json_file in json_files:
        process_json_file(json_file)

if __name__ == "__main__":
    main()
