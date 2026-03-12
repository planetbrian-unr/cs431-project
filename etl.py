# ETL the dataset from a zip file into a database with csv intermediaries
# should be easy to adapt for Spark SQL
# Brian Wu

# built-in
import sys
import csv
import sqlite3
import os
import zipfile

## UNZIP
# used chatgpt
def unzip_get_depth(dataset_id: str) -> int:
    # unzip <id>.zip
    zipfile.ZipFile(f"{dataset_id}.zip", "r").extractall()

    # generator of numeric parts from files named "<number>.txt"
    stems = (
        int(name.split('.')[0])
        for name in os.listdir(dataset_id)
        if name.endswith('.txt') and name.split('.')[0].isdigit()
    )
    return max(stems) + 1

### ETL
# the text file is delimited with tabs. consolidate all into one master
# perform basic sanity checks, such as altering rows that don't form a full video tuple
def extract(dataset_id:str, depth_level:int) -> None:
    # open all i_txt from 0 to d_l exclusive (d_l-1) and store their rows in valid_rows
    valid_rows = []
    for i in range(0, depth_level):
        i_txt:str = f'{dataset_id}/{i}.txt'

        with open(i_txt, 'r') as infile:
            reader = csv.reader(infile, delimiter='\t')
            for row in reader:
                # add padding if necessary
                if len(row) == 1:
                    row.extend(["Unknown", "0", "None", "0", "0", "0.00" , "0", "0"])
                valid_rows.append(row)
        
    # add to master.csv in bulk from list. reduces runtime duration
    master_csv:str = f'{dataset_id}/master.csv'
    with open(master_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(valid_rows)

# transform the master into 3 files
def transform(dataset_id:str) -> None:
    # define files
    users_csv:str = f'{dataset_id}/users.csv'
    categories_csv:str = f'{dataset_id}/categories.csv'
    master_csv:str = f'{dataset_id}/master.csv'
    videos_csv:str = f'{dataset_id}/videos.csv'

    # create sets to store values from categories/users (deduplicated by nature)
    categories:set = set()
    users:set = set()

    # open master_csv. grab specific columns for sets, and store the first 9 rows as a video tuple
    with open(master_csv, 'r') as infile, open(videos_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            users.add(row[1])
            categories.add(row[3])
            writer.writerow(row[0:9])

    # write deduplicated set of categories to a csv
    with open(categories_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(([c] for c in categories))

    # likewise. there are several videos with the same poster
    with open(users_csv, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerows([[u] for u in users])

# general load function
def load(dataset_id:str, db_name:str) -> None:
    # define files
    users_csv:str = f'{dataset_id}/users.csv'
    categories_csv:str = f'{dataset_id}/categories.csv'
    master_csv:str = f'{dataset_id}/master.csv'
    videos_csv:str = f'{dataset_id}/videos.csv'

    # create a database, automatically manage closing it
    with sqlite3.connect(f'{dataset_id}/{db_name}') as con:
        cur:sqlite3.Cursor = con.cursor()

        create_tables(cur)
        load_basic_table(cur, categories_csv, 'Category', 'category')
        load_basic_table(cur, users_csv, 'User', 'username')
        load_video_table(cur, videos_csv)
        load_relations_table(cur, master_csv)
        
        con.commit()

### Helper functions for LOAD
# create all tables
def create_tables(cur:sqlite3.Cursor) -> None:
    cur.execute("""
                CREATE TABLE Category (
                    id INTEGER PRIMARY KEY,
                    category VARCHAR(128)
                ); 
                """)
    cur.execute("""
                CREATE TABLE Uploader (
                    id INTEGER PRIMARY KEY,
                    username VARCHAR(128)
                ); 
                """)
    cur.execute("""
                CREATE TABLE Video (
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
    cur.execute("""
                CREATE TABLE Relation (
                    video_id CHAR(11),
                    related_id CHAR(11),
                
                    PRIMARY KEY (video_id, related_id),                
                    FOREIGN KEY (video_id) REFERENCES Video(id),
                    FOREIGN KEY (related_id) REFERENCES Video(id)
                );
                """)

# load everything in from csv
def load_basic_table(cur:sqlite3.Cursor, file_path:str, table_name:str, column:str) -> None:
    with open(file_path) as file:
        reader = csv.reader(file)
        cur.executemany(f"""
                        INSERT INTO {table_name} ({column})
                        VALUES (?);
                        """, reader)

# create video table and load everything in from csv
def load_video_table(cur:sqlite3.Cursor, file_path:str) -> None:
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
    with open(file_path, newline='') as file:
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
        print("Usage: python txt-to-csv.py <dataset_id> <db_name>")
        return

    # store command-line arguments
    dataset_id:str = sys.argv[1]
    db_name:str = sys.argv[2]

    # UNZIP the datset and get depth
    depth_level:int = unzip_get_depth(dataset_id)

    # EXTRACT from 0 to defined depth level    
    extract(dataset_id, depth_level)
    
    # TRANSFORM then into table-csv
    transform(dataset_id)
    
    # LOAD all tables
    load(dataset_id, db_name)


if __name__ == '__main__':
    main()
