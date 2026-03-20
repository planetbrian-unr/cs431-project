# Spark
Documentation for how our project utilizes Apache Spark

## JDBC Driver
The JDBC Driver is necessary for the movement of the tables and data in our SQLite database over to Apache Spark and Pyspark for further manipulation and analysis. It acts as translation between Spark and SQLite where it can be given a request by Spark and will perform all of the work in order to produce the data in response. This driver can be found under the main directory of our repository as *sqlite-jdbc-3.51.2.0.jar*. This file was found from the [sqlite-jdbc GitHub repository](https://github.com/xerial/sqlite-jdbc). 

## JDK
Java Development Kit (JDK) is required for running Spark/Pyspark. For a Unix based system, this can be installed easily using a few terminal commands as seen in our README. Coupled with the installation of Pyspark through a pip command, this covers all necessary for launching an instance of Spark in Python.

## Initialzation and Database Loading
An initialization function was written to launch a session of Spark. This session utilizes a few Spark commands, before returning the instance of the session back to the main function, so that it can be passed into any other function that requires it. Once of which being the function to load the database table from an SQLite file into Spark. This file is passed in as a command line parameter when the code is ran. A protocol tag is appended to the file name, which is then passed into a Spark call that reads the data and pulls out a specific table. We run this function four times, one for each of the tables we have in our database. Now, the data is ready for analysis. 