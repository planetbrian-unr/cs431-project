# extract data from csv files
# Brian Wu
# 

import sys
import csv

# O(3n) -> O(n)
# Extract i_csv files and write to separate table csv files
def extract_data(i_csv:str, categories_csv:str, users_csv:str, videos_csv:str) -> None:
    categories:set = set()
    users:set = set()

    # O(n)
    with open(i_csv, 'r') as infile, open(videos_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            categories.add(row[3])
            users.add(row[1])
            writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]])

    # O(n)            
    with open(categories_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for category in categories:
            writer.writerow([category])

    # O(n)
    with open(users_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for user in users:
            writer.writerow([user])

def main() -> None:
    if len(sys.argv) == 3:
        dataset_dir:str = sys.argv[1]
        depth_level:int = int(sys.argv[2])

        # run time O(i * 3n) -> O(n^2)
        # extract data from i_csv files and write to separate table csv files
        for i in range(0, depth_level):
            i_csv:str = f'{dataset_dir}/{i}.csv'
            categories_csv:str = f'{dataset_dir}/categories.csv'
            users_csv:str = f'{dataset_dir}/users.csv'
            videos_csv:str = f'{dataset_dir}/videos.csv'
            extract_data(i_csv, categories_csv, users_csv, videos_csv)

    else:
        print("Usage: python csv-categories.py <dataset_dir> <depth_level>")

if __name__ == '__main__':
    main()
