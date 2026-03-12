# CS 431 Project R11

## Description
We will be using the datasets stored [here](http://netsg.cs.sfu.ca/youtubedata/) to implement a Youtube data analyzer with a Network Aggregation function, which efficiently reports the following statistics of the Youtube video network:
- Degree distribution (including in-degree and out-degree); average degree, maximum, and minimum degree
- Categorized statistics: frequency of videos partitioned by a search condition: categorization, size of videos, view count, etc.

To accomplish this project, Spark and Python will be used.

## How to run
Presuming you do not have Spark installed on your Unix-like system and that you have only cloned this repository...

### Fetch
1. Edit the `dataset_name` variable in `fetch.sh` to reflect the desired dataset.
2. Run `fetch.sh` to get the dataset, Spark, and the SQLite3 JDBC .jar.

### ETL 
3. Run `python etl.py <dataset_id> <database_name>`.
4. The deliverables are deduplicated and cleaned CSVs and a SQLite3 database that stores everything correctly

### Spark
5. Install Spark (how?)
6. Run `spark.py`

## Resources
- [Spark documentation](https://spark.apache.org/docs/latest/index.html)

## Credits
Team members: Brian Wu, Matthew Gaskell
