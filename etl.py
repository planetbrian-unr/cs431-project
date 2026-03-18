# ETL the dataset from a zip file into a database with csv intermediaries
# Brian Wu

# built-in
import csv
import sqlite3
import os
import sys
import zipfile

# pip
import wget

# get dataset and unzip it
def get_dataset(dataset_id:str) -> None:
    wget.download(f"https://netsg.cs.sfu.ca/youtubedata/{dataset_id}.zip", bar=None)
    zipfile.ZipFile(f"{dataset_id}.zip", "r").extractall()

# get depth. used ai
def get_dataset_depth(dataset_id:str) -> int:
    # generator of numeric parts from files named "<number>.txt"
    stems = (
        int(name.split('.')[0])
        for name in os.listdir(dataset_id)
        if name.endswith('.txt') and name.split('.')[0].isdigit()
    )
    return max(stems) + 1

# the text file is delimited with tabs. consolidate all into one master csv
def extract(dataset_id:str, depth_level:int, master_csv:str) -> None:
    # open all i_txt from 0 to d_l exclusive (d_l-1) and store their rows in valid_rows
    valid_rows = []
    for i in range(0, depth_level):
        i_txt:str = f'{dataset_id}/{i}.txt'
        with open(i_txt, 'r') as infile:
            reader = csv.reader(infile, delimiter='\t')
            for row in reader:
                # sanity check: add padding if necessary to form a full video tuple
                if len(row) == 1:
                    row.extend(["Unknown", "0", "None", "0", "0", "0.00" , "0", "0"])
                valid_rows.append(row)
        
    # add to master.csv in bulk from list. reduces runtime duration
    with open(master_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(valid_rows)

# transform the master into 3 files
def transform(files:list[str]) -> None:
    # create sets to store values from categories/users (deduplicated by nature)
    categories:set = set()
    users:set = set()

    # open master_csv. grab specific columns for sets, and store the first 9 rows as a video tuple
    with open(files[0], 'r') as infile, open(files[3], 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            users.add(row[1])
            categories.add(row[3])
            writer.writerow(row[0:9])

    # write deduplicated set of categories to a csv
    with open(files[2], 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(([c] for c in categories))

    # likewise. there are several videos with the same poster
    with open(files[1], 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows([[u] for u in users])

# general load function
def load(files:list[str]) -> None:
    # create a database, automatically manage closing it
    with sqlite3.connect(files[4]) as con:
        cur:sqlite3.Cursor = con.cursor()

        load_relations_table(cur, files[0])
        load_basic_table(cur, files[1], 'User', 'username')
        load_basic_table(cur, files[2], 'Category', 'category')
        load_video_table(cur, files[3])
        
        con.commit()

# load everything in from csv
def load_basic_table(cur:sqlite3.Cursor, csv_path:str, table_name:str, column:str) -> None:
    # create table
    cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    {column} VARCHAR(128) UNIQUE
                ); 
                """)
    
    # load table from csv
    with open(csv_path) as file:
        reader = csv.reader(file)
        cur.executemany(f"""
                        INSERT INTO {table_name} ({column})
                        VALUES (?);
                        """, reader)

# create video table and load everything in from csv
def load_video_table(cur:sqlite3.Cursor, videos_csv:str) -> None:
    # create table
    cur.execute("""
                CREATE TABLE IF NOT EXISTS Video (
                    id CHAR(11) PRIMARY KEY,
                    uploader INTEGER,
                    age INTEGER,
                    category INTEGER,
                    length INTEGER,
                    views INTEGER,
                    rate REAL,
                    ratings INTEGER,
                    comments INTEGER,
                
                    FOREIGN KEY (uploader) REFERENCES User(id),
                    FOREIGN KEY (category) REFERENCES Category(id)
                );
                """)
    
    # use dictionaries to translate categories/users into integers for FK
    cur.execute("SELECT * FROM User;")
    user_dictionary:dict[str, int] = {}
    for (id, username) in cur:
        user_dictionary[username] = id

    cur.execute("SELECT * FROM Category;")
    category_dictionary:dict[str, int] = {}
    for (id, description) in cur:
        category_dictionary[description] = id

    rows_to_insert = []
    with open(videos_csv, newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            # Build a tuple matching the Video column order
            rows_to_insert.append((
                row[0],
                user_dictionary.get(row[1]),
                row[2],
                category_dictionary.get(row[3]),
                row[4],
                row[5],
                row[6],
                row[7],
                row[8]
            ))

    cur.executemany("""
                    INSERT INTO Video
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, rows_to_insert)

# these will be the vertices between nodes
def load_relations_table(cur:sqlite3.Cursor, master_csv:str) -> None:
    # create table
    cur.execute("""
                CREATE TABLE IF NOT EXISTS Relation (
                    video_id CHAR(11),
                    related_id CHAR(11),
                
                    PRIMARY KEY (video_id, related_id),                
                    FOREIGN KEY (video_id) REFERENCES Video(id),
                    FOREIGN KEY (related_id) REFERENCES Video(id)
                );
                """)
    
    # load table from master
    with open(master_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            # create pairs. up to 20 recommended videos, but there may be 0. 0-20.
            relation = ((row[0], cell) for cell in row[9:30])
            cur.executemany("""
                            INSERT INTO Relation
                            VALUES (?, ?);
                            """, relation)

def main() -> None:
    # if command-line arguments not exactly 3, gracefully fail
    if len(sys.argv) != 3:
        print("Usage: python etl.py <dataset_id> <db_name>")
        return

    # store command-line arguments
    dataset_id:str = sys.argv[1]
    db_name:str = sys.argv[2]

    # establish list of desired files
    files:list[str] = [
        f"{dataset_id}/master.csv",
        f"{dataset_id}/users.csv",
        f"{dataset_id}/categories.csv",
        f"{dataset_id}/videos.csv",
        f"{dataset_id}/{db_name}"
    ]

    # unzip dataset files and get depth
    get_dataset(dataset_id)
    depth:int = get_dataset_depth(dataset_id)

    # etl
    extract(dataset_id, depth, files[0])
    transform(files)
    load(files)
    

if __name__ == '__main__':
    main()