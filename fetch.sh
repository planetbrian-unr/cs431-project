#!/bin/bash

# set dataset name to get
dataset_name="0302" # 0302 is the smallest one
wget https://netsg.cs.sfu.ca/youtubedata/$dataset_name.zip

# get corresponding jdbc driver for sqlite version installed to system
sqlite_version=$(sqlite3 --version | cut -d ' ' -f 1)
wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/$sqlite_version.0/sqlite-jdbc-$sqlite_version.0.jar

# get and install spark
# test for java installation
# MATT