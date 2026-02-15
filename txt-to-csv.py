# Convert txt to csv for easier data manipulation
# Brian Wu
# likely might be O(n^2)

import sys
import csv

# run time O(n), where n is the number of rows in the txt file
# Remove rows with less than 9 columns (incomplete video row) and write to csv file
def clean_data(txt_file:str, csv_file:str) -> None:
    with open(txt_file, 'r') as infile, open(csv_file, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)

        for row in reader:
            if len(row) < 9:
                continue
            writer.writerow(row)

def main() -> None:
    if len(sys.argv) == 3:
        dataset_dir:str = sys.argv[1]
        depth_level:int = int(sys.argv[2])

        # run time O(i * n) -> O(n^2)
        # open 0.txt, 1.txt, ..., depth_level-1.txt and write to 0.csv, 1.csv, ..., depth_level-1.csv
        for i in range(0, depth_level):
            txt_file:str = f'{dataset_dir}/{i}.txt'
            csv_file:str = f'{dataset_dir}/{i}.csv'
            clean_data(txt_file, csv_file)

    else:
        print("Usage: python txt-to-csv.py <dataset_dir> <depth_level>")

if __name__ == '__main__':
    main()
