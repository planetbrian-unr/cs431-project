# ETL the dataset into a database with csv intermediaries
# should be easy to adapt for Spark SQL
# Brian Wu

import sys
import csv
import sqlite3

# the text file is delimited with tabs. consolidate all into one master
# perform basic sanity checks, such as removing rows that don't form a full video
def extract(i_txt:str, master_csv:str) -> None:
    with open(i_txt, 'r') as infile, open(master_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)

        # add each valid (>=9 columns) row from the txt to the csv
        # ATTENTION: should i do this
        valid_rows = (row for row in reader if len(row) >= 9)
        writer.writerows(valid_rows)

# transform the master into 3 files
def transform(master_csv:str, categories_csv:str, users_csv:str, videos_csv:str) -> None:
    # create sets to store values from categories/users (deduplicated by nature)
    categories:set = set()
    users:set = set()

    # open master_csv. grab specific columns for sets, and store the first 9 rows as a video tuple
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

# create all tables
def create_tables(cur:sqlite3.Cursor) -> None:
    cur.execute("""
                CREATE TABLE Category (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category VARCHAR(128)
                ); 
                """)
    cur.execute("""
                CREATE TABLE User (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
# ISSUE: i cant get the Video.rate column to import correctly. 4.xx becomes 4, etc.
def load_video_table(cur:sqlite3.Cursor, file_path:str) -> None:
    # use dictionaries to translate categories/users into integers for FK
    cur.execute("SELECT * FROM User;")
    user_dictionary = {}
    for (id, username) in cur:
        user_dictionary[username] = id

    cur.execute("SELECT * FROM Category;")
    category_dictionary = {}
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
                row[6], # HERE
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
            # up to 20 recommended videos, but there may be 0. 0-20.
            for cell in row[9:30]:
                    cur.execute("""
                                INSERT INTO Relation
                                VALUES (?, ?);
                                """, (row[0], cell))

def main() -> None:
    # if command-line arguments not exactly 3, gracefully fail
    if len(sys.argv) != 4:
        print("Usage: python txt-to-csv.py <dataset_dir> <depth_level> <db_name>")
        return

    # store command-line arguments
    dataset_dir:str = sys.argv[1]
    depth_level:int = int(sys.argv[2])
    db_name:str = sys.argv[3]

    # EXTRACT from 0 to defined depth level
    for i in range(0, depth_level):
        # convert i.txt into master.csv...
        extract(f'{dataset_dir}/{i}.txt', 
                f'{dataset_dir}/master.csv')
    
    # TRANSFORM then into table-csv
    transform(f'{dataset_dir}/master.csv', 
            f'{dataset_dir}/categories.csv', 
            f'{dataset_dir}/users.csv', 
            f'{dataset_dir}/videos.csv')
    
    # create a database
    con:sqlite3.Connection = sqlite3.connect(f'{dataset_dir}/{db_name}')
    cur:sqlite3.Cursor = con.cursor()    

    # LOAD all tables
    create_tables(cur)
    load_basic_table(cur, f'{dataset_dir}/categories.csv', 'Category', 'category')
    load_basic_table(cur, f'{dataset_dir}/users.csv', 'User', 'username')
    load_video_table(cur, f'{dataset_dir}/videos.csv')
    load_relations_table(cur, f'{dataset_dir}/master.csv')
    
    con.commit()
    con.close()


if __name__ == '__main__':
    main()
