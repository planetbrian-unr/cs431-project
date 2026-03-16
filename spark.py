# import the sqlite dataset into spark via pyspark and spark sql
# TODO: graph generation
# Matthew Gaskell

# built-in
import sys

# pip imports
from pyspark.sql import SparkSession, DataFrame
from graphframes import GraphFrame
import networkx as nx
import matplotlib.pyplot as plt
import pandas

# SparkSession initialization
def initialize_spark_session() -> SparkSession:
    spark_session = SparkSession.builder.appName("CS431Project").config("spark.jars", "sqlite-jdbc-3.51.2.0.jar").getOrCreate()

    return spark_session

# Load a table into spark, and return the dataframe for further use
def load_table_into_spark(spark_session:SparkSession, db_name:str, table_name:str) -> DataFrame:
    # set parameters and load a table into a dataframe
    spark_sqlite_con = spark_session.read.format("jdbc").option("driver", "org.sqlite.JDBC")
    sql_table = spark_sqlite_con.option("url", db_name).option("dbtable", table_name)
    df = sql_table.load()

    return df

# Loading the four tables that are present in the database produced in etl.py
def load_sqlite_tables(spark_session:SparkSession, db_name:str) -> list[DataFrame]:
    dfs:list[DataFrame] = []

    table_names:list[str] = ["Category", "User", "Video", "Relation"]
    for name in table_names:
        df = load_table_into_spark(spark_session, db_name, name)
        dfs.append(df)

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
    # make the s_s reusable without needing a getOrCreate call
    spark_session = initialize_spark_session()
    spark_tables = load_sqlite_tables(spark_session, database)

# if __name__ == '__main__':
#     main()