# CS 431 Project R11

## Description
We will be using the datasets stored [here](http://netsg.cs.sfu.ca/youtubedata/) to implement a Youtube data analyzer with a Network Aggregation function, which efficiently reports the following statistics of the Youtube video network:
- Degree distribution (including in-degree and out-degree); average degree, maximum, and minimum degree
- Categorized statistics: frequency of videos partitioned by a search condition: categorization, size of videos, view count, etc.

To accomplish this project, PySpark and Python will be used.

## How To Run
Presuming you do not have Spark installed on your Unix-like system and that you have only cloned this repository...

1. Run `pip install -r requirements.txt`. This ensures wget, Pyspark, and other necessary Python libraries are installed.
2. If you do not already have an installation of Java, run the following commands in your Unix or WSL terminal:
    - `sudo apt update && sudo apt install openjdk-17-jdk -y`
    - `echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc`
    - `echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc`
    - `source ~/.bashrc`
3. Run `python main.py <dataset_id> <database_name>`.

The dataset will be retrieved and transformed into an SQLite database. After this, the menu of options will appear. You may select [1] for degree distribution statistics reporting, [2] for simple graph generation, [3] for categorized statistics searching and reporting, or [3] to exit the program. 

## Extras
If you would rather run each stage independently, uncomment the `import sys` line and `main()` in etl.py, frequency.py, and spark.py.
If you already have produced a database with our code and want to utilize either of the two aforementioned functionalities more efficiently and by themselves, run either `python spark.py <database_file_name.db>` or `python frequency.py <database_file_name.db>`. 

## Resources
- [Spark Documentation](https://spark.apache.org/docs/latest/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [GraphFrames](https://graphframes.io/02-quick-start/01-installation.html)

## Credits
Team members: Brian Wu, Matthew Gaskell
