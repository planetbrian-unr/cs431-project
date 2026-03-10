# Spark
Documentation for how our project utilizes Apache Spark

## JDBC Driver
The JDBC Driver is necessary for the movement of the tables and data in our SQLite database over to Apache Spark and Pyspark for further manipulation and analysis. It acts as translation between Spark and SQLite where it can be given a request by Spark and will perform all of the work in order to produce the data in response. This driver can be found under the main directory of our repository as *sqlite-jdbc-3.51.2.0.jar*. This file was found from the [sqlite-jdbc GitHub repository](https://github.com/xerial/sqlite-jdbc). 

## Initialzation and Database Loading
