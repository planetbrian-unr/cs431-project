# Spark
Documentation for how our project utilizes Apache Spark and other related libraries.

## JDBC Driver
The JDBC Driver is necessary for the movement of the tables and data in our SQLite database over to Apache Spark and Pyspark for further manipulation and analysis. It acts as translation between Spark and SQLite where it can be given a request by Spark and will perform all of the work in order to produce the data in response. This driver can be found under the main directory of our repository as *sqlite-jdbc-3.51.2.0.jar*. This file was found from the [sqlite-jdbc GitHub repository](https://github.com/xerial/sqlite-jdbc). 

## JDK
Java Development Kit (JDK) is required for running Spark/Pyspark. For a Unix based system, this can be installed easily using a few terminal commands as seen in our README. Coupled with the installation of Pyspark through a pip command, this covers all necessary for launching an instance of Spark in Python.

## Initialzation and Database Loading
An initialization function was written to launch a session of Spark. This session utilizes a few Spark commands, before returning the instance of the session back to the main function, so that it can be passed into any other function that requires it. Once of which being the function to load the database table from an SQLite file into Spark. This file is passed in as a command line parameter when the code is ran. A protocol tag is appended to the file name, which is then passed into a Spark call that reads the data and pulls out a specific table. We run this function four times, one for each of the tables we have in our database. Now, the data is ready for analysis. 

## Network Aggregation and Degree Reporting
We are aggregating the data and building our graph network on the relations between the videos. The data comes with a table that contains a list of video ids paired with an id from a video that it is related to. Plugging the data from this table into nodes and edges, which are then plugged into a GraphFrames object, allows us to then utilize functions that can report statistics of these relations. Here, we are reporting about degrees, which is the number of edges coming off of a node in a graph. Our program prints out the first 20 rows of tables listing the number of in-degrees of videos and the number of out-degrees of tables (i.e. how many edges enter a node and how many edges leave a node). Additionally, the average number of degrees across the whole graph is calculated and printed. Finally, the minimum and maximum degree values are printed, along with the video id that is represented by those values. 

## NetworkX Plotting
Using NetworkX, a function has been created that plots the aggregated network going out from a provided video id. This is set as default to the first video in the data list. Additionally, a radius value is used that shows how far the graph will extend from this centroid. This is set to 3, as anything higher results in either a graph that is not readable with additional computational penalties. This graph is saved to a PNG file as displaying this graph through WSL is not straightforward. An example graph is present in the main repository directory as *networkx_graph.png*. 

## Debugging Spark/GraphFrames
In *spark.py*, commented out lines are present and labeled as debug lines. These lines get and print the version of Spark that is being ran, and the version of Scala that is being ran. While these two will be different values, a mismatch can lead to errors which will crash the program. If this is the case, check the two versions, and use them to figure out the GraphFrames JAR that should be used for the code to work properly (found on line 20 of *spark.py*).