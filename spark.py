# Library Imports
from pyspark.sql import SparkSession, DataFrame
from graphframes import GraphFrame
import networkx as nx
import matplotlib.pyplot as plt
import pandas
import sys

# SparkSession initialization
def initialize_spark_session() -> None:
    SparkSession.builder.appName("CS431Project").config("spark.jars", "sqlite-jdbc-3.51.2.0.jar").getOrCreate()

# Load a table into spark, and return the dataframe for further use
def load_table_into_spark(db_name:str, table_name:str) -> DataFrame:
    # locate present session from initialization function
    spark_session = SparkSession.builder.getOrCreate()
    spark_sqlite_con = spark_session.read.format("jdbc").option("driver", "org.sqlite.JDBC")
    sql_table = spark_sqlite_con.option("url", db_name).option("dbtable", table_name)
    
    df = sql_table.load()
    return df

# Loading the four tables that are present in the database produced in etl.py
def load_sqlite_tables(db_name: str) -> list:
    dfs = []

    category_df = load_table_into_spark(db_name, "Category")
    dfs.append(category_df)
    user_df = load_table_into_spark(db_name, "User")
    dfs.append(user_df)
    video_df = load_table_into_spark(db_name, "Video")
    dfs.append(video_df)
    relation_df = load_table_into_spark(db_name, "Relation")
    dfs.append(relation_df)

    return dfs

def network_aggregation():
    pass

def graph_and_display(graph: GraphFrame, title: str):
    df = graph.edges.select("src", "dst").toPandas()
    nx_graph = nx.from_pandas_edgelist(df, source="src", target="dst")

    plt.figure()
    plt.title(title)
    nx.draw(nx_graph)
    plt.show()

# Main
def main() -> None:
    # Check for passed in cmdline argument
    if len(sys.argv) != 2:
        print("Usage: python spark.py <database_file_name.db>")

    # Passed in arg = sqlite database file
    database:str = sys.argv[1]

    # Spark init and data loading
    initialize_spark_session()
    spark_tables = load_sqlite_tables(database)