# CS 431 Project R11

## Description
We will be using the datasets stored [here](http://netsg.cs.sfu.ca/youtubedata/) to implement a Youtube data analyzer with a Network Aggregation function, which efficiently reports the following statistics of the Youtube video network:
- Degree distribution (including in-degree and out-degree); average degree, maximum, and minimum degree
- Categorized statistics: frequency of videos partitioned by a search condition: categorization, size of videos, view count, etc.

To accomplish this project, Spark and Python will be used.

## How To Run
Presuming you do not have Spark installed on your Unix-like system and that you have only cloned this repository...

### ETL
1. Run `pip install -r requirements.txt` for wget.
2. Run `python etl.py <dataset_id> <database_name>`.
3. The deliverables are deduplicated and cleaned CSVs and a SQLite3 database that stores everything correctly

### Spark
4. Run `pip install -r requirements.txt` to ensure that Pyspark and other necessary libraries are installed and ready for use. Running this in a Unix enrivonment (WSL if on Windows) will ensure that most of the necessary supporting binaries are also installed. Much further efforts are required to make Spark run on Windows, as it is heavily built for Linux, and we recommend switching the runtime environment to Unix before attempting this. 
5. Once this is done, Java must be set up for the installation to run successfully. If you do not already have an instantiation of Java, running the following commands in your Unix or WSL terminal will achieve this:
    - `sudo apt update`
    - `sudo apt install openjdk-17-jdk -y`
    - `echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc`
    - `echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc`
    - `source ~/.bashrc`
    - `echo $JAVA_HOME`
    - `java -version`

    The previous two commands are for validation, the first should display a path which looks like `/usr/lib/jvm/java-17-openjdk-amd64` and the second should display some information indicating that the version of Java is 17. 
6. Run `spark.py`

## Resources
- [Spark Documentation](https://spark.apache.org/docs/latest/index.html)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [GraphFrames](https://graphframes.io/02-quick-start/01-installation.html)

## Credits
Team members: Brian Wu, Matthew Gaskell
