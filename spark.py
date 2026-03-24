# import the sqlite dataset into spark via pyspark and spark sql
# TODO: graph generation
# Matthew Gaskell

# built-in
import sys

# pip imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, asc, desc
from graphframes import GraphFrame
import networkx as nx
import matplotlib.pyplot as plt

# SparkSession initialization
def initialize_spark_session() -> SparkSession:
    spark_session = (SparkSession.builder
        .appName("CS431Project")
        .config("spark.jars", "sqlite-jdbc-3.51.2.0.jar")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark3_2.12:0.10.1")
        .config("spark.sql.extensions", "graphframes.GraphFrames")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")   # ← change to "WARN"/"INFO" as needed
    
    return spark_session

# Load a table into spark, and return the dataframe for further use
def load_table_into_spark(spark_session:SparkSession, db_name:str, table_name:str) -> DataFrame:
    # set parameters and load a table into a dataframe
    protocol_db = "jdbc:sqlite:" + db_name  # Required format for Spark to understand what it's working with
    spark_sqlite_con = spark_session.read.format("jdbc").option("driver", "org.sqlite.JDBC").option("url", protocol_db)
    sql_table = spark_sqlite_con.option("dbtable", table_name)
    df = sql_table.load()

    return df

# Loading the four tables that are present in the database produced in etl.py
def load_sqlite_tables(spark_session:SparkSession, db_name:str) -> list: # Adding [DataFrame] after list errors for me
    dfs:list[DataFrame] = []

    table_names:list[str] = ["Category", "User", "Video", "Relation"]
    for name in table_names:
        df = load_table_into_spark(spark_session, db_name, name)
        dfs.append(df)

    return dfs

def network_aggregation(video_table:DataFrame, relation_table:DataFrame) -> GraphFrame:
    # Take the video id column from the video table
    nodes = video_table.select(col("id"))
    # Select values from the relation table to form edges between video nodes
    edges = relation_table.select(col("src"), col("dst"))
    # edges = relation_table.select(col("video_id").alias("src"), col("related_id").alias("dst"))
    
    # Plug into GraphFrame object and return
    graph = GraphFrame(nodes, edges)

    return graph

def degree_reporting(graph: GraphFrame) -> None:
    # In and out degree reporting
    print(f"In Degrees (first 20 rows):")
    graph.inDegrees.show()

    print(f"Out Degrees (first 20 rows):")
    graph.outDegrees.show()

    # Average degree reporting
    num_nodes = graph.vertices.count()
    num_edges = graph.edges.count()
    average_degree = num_edges / num_nodes
    print(f"Average Degrees per Node: {average_degree}.", end="\n\n")

    # Min and max degree reporting
    print(f"Minimum Degrees:")
    graph.degrees.orderBy(col("degree").asc()).limit(1).show()

    print(f"Maximum Degrees:")
    graph.degrees.orderBy(col("degree").desc()).limit(1).show()

def graph_and_display(graph: GraphFrame, title: str) -> None:
    df = graph.edges.select("src", "dst").toPandas()
    nx_graph = nx.from_pandas_edgelist(df, source="src", target="dst")

    plt.figure()
    plt.title(title)
    nx.draw(nx_graph, node_size=200, edge_color='black', node_color='green', with_labels=True, font_size=15)
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

    # Debug Lines 
    # print(f"SPARK VERSION: {spark_session.version}")
    # print(f"SCALA VERSION: {spark_session._jvm.scala.util.Properties.versionString()}")

    spark_tables = load_sqlite_tables(spark_session, database)
    video_relation_graph = network_aggregation(spark_tables[2], spark_tables[3])
    degree_reporting(video_relation_graph)
    # graph_and_display(agg_graph, "Title")

if __name__ == '__main__':
    main()