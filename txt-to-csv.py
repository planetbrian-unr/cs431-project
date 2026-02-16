# Convert txt to a set of csv's for easier data manipulation
# Brian Wu

import sys
import csv

def convert(i_txt:str, i_csv:str) -> None:
    with open(i_txt, 'r') as infile, open(i_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)

        # add each valid (>=9 columns) row from the txt to the csv
        # ATTENTION: should i do this
        valid_rows = (row for row in reader if len(row) >= 9)
        writer.writerows(valid_rows)

def extract(master_csv:str, categories_csv:str, users_csv:str, videos_csv:str) -> None:
    # create sets to store values from categories/users (deduplicated by nature)
    categories:set = set()
    users:set = set()

    # open i_csv. grab specific columns for sets, and store the first 9 rows as a video tuple
    with open(master_csv, 'r') as infile, open(videos_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            categories.add(row[3])
            users.add(row[1])
            writer.writerow(row[0:9])

    # write deduplicated set of categories to a csv
    with open(categories_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(([c] for c in categories))

    # likewise. there are several videos with then same poster
    with open(users_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows([[u] for u in users])

def main() -> None:
    # if command-line arguments not exactly 3, gracefully fail
    if len(sys.argv) != 3:
        print("Usage: python txt-to-csv.py <dataset_dir> <depth_level>")
        return

    # store command-line arguments
    dataset_dir:str = sys.argv[1]
    depth_level:int = int(sys.argv[2])

    # from 0 to defined depth level...
    for i in range(0, depth_level):
        # convert i.txt into master.csv...
        convert(f'{dataset_dir}/{i}.txt', 
                f'{dataset_dir}/master.csv')
    
    # ...then into table-csv
    extract(f'{dataset_dir}/master.csv', 
            f'{dataset_dir}/categories.csv', 
            f'{dataset_dir}/users.csv', 
            f'{dataset_dir}/videos.csv')


if __name__ == '__main__':
    main()
