# CS 431 Project R11

## Description
We will be using the data stored [here](http://netsg.cs.sfu.ca/youtubedata/) to implement a Youtube data analyzer with a Network Aggregation function, which efficiently reports the following statistics of the Youtube video network:
- Degree distribution (including in-degree and out-degree); average degree, maximum, and minimum degree
- Categorized statistics: frequency of videos partitioned by a search condition: categorization, size of videos, view count, etc.

To accomplish this project, Spark and Python will be used.

## How to run 
1. Extract a dataset into a directory d, noting the depth level
2. Run `python txt-to-csv.py <d> <depth_level>`
3. Run `python csv-to-sqlite.py <d> <database_name>`
4. The deliverables from 2 and 3 are deduplicated and cleaned CSVs and a SQLite3 database that stores everything correctly
5. Spark can use SQLite files through a JDBC driver

## Credits
Team members: Brian Wu, Matthew Gaskell
