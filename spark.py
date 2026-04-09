# import the sqlite dataset into spark via pyspark and spark sql and perform network aggregation and degree distribution reporting
# Matthew Gaskell

# built-in
# import sys

# pip imports
import wget
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, asc, desc
from graphframes import GraphFrame
import networkx as nx
import matplotlib.pyplot as plt

# TODO: ATTENTION! notice spark version! maybe implement checks?
# SparkSession initialization
def initialize_spark_session() -> SparkSession:
    # prerequisite download
    wget.download("https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.51.2.0/sqlite-jdbc-3.51.2.0.jar", bar=None)

    spark_session = (SparkSession.builder
        .appName("CS431Project")
        .config("spark.jars", "sqlite-jdbc-3.51.2.0.jar")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark3_2.12:0.10.1")
        # .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.10.0") # github codespaces' pyspark is version 4. i need this for myself
        .config("spark.sql.extensions", "graphframes.GraphFrames")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")   # change to "WARN"/"INFO" as needed
    
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

# Create the network graph of data based on videos that are related to each other using the video and relation tables. 
def network_aggregation(video_table:DataFrame, relation_table:DataFrame) -> GraphFrame:
    # Video.id as Nodes, Relation.src/dst as Edges
    nodes = video_table.select(col("id"))
    edges = relation_table.select(col("src"), col("dst"))
    
    # Plug into GraphFrame object and return
    graph = GraphFrame(nodes, edges)

    return graph

# This function reports the degree distribution of the video relations in the form of in degrees, out degrees, average degrees, minimum degrees,
# and maxmimum degrees. The in and out degrees are truncated to the first 20 rows.
def degree_reporting(graph:GraphFrame) -> None:
    # In and out degree reporting
    print(f"In Degrees (first 20 rows):")
    graph.inDegrees.show()

    print(f"Out Degrees (first 20 rows):")
    graph.outDegrees.show()

    # Average degree reporting
    num_nodes:int = graph.vertices.count()
    num_edges:int = graph.edges.count()
    average_degree:float = num_edges / num_nodes
    print(f"Average Degrees per Node: {average_degree}.", end="\n\n")   # is degree really the right word?

    # Min and max degree reporting
    print(f"Minimum Degrees:")
    graph.degrees.orderBy(col("degree").asc()).limit(1).show()

    print(f"Maximum Degrees:")
    graph.degrees.orderBy(col("degree").desc()).limit(1).show()

# This function graphs and displays the network resulting from one video. This is done to show an example of the network aggregation done by Spark.
# The video can be selected by modifying the list entry value, and the figure can be shown if desired. By default it is saved to a PNG file.  
def graph_and_display(graph: GraphFrame, title: str) -> None:
    df = graph.edges.select("src", "dst").toPandas()
    nx_graph = nx.from_pandas_edgelist(df, source="src", target="dst")

    plt.figure()
    plt.title(title)

    # Pick a central node, graph around it.
    centroid = list(nx_graph.nodes)[0]
    nx_centroid_graph = nx.ego_graph(nx_graph, centroid, radius=3)
    nx.draw(nx_centroid_graph, node_size=300, edge_color='black', node_color='turquoise', with_labels=True, font_size=5)
    
    # Uncomment if desired, I commented as showing through WSL is a struggle.
    # plt.show()
    plt.savefig("networkx_graph.png", format="PNG", dpi=300)

# Main (used for debugging/testing when program not ran from main.py, requires existing database file)
# def main() -> None:
#     # Check for passed in cmdline argument
#     if len(sys.argv) != 2:
#         print("Usage: python spark.py <database_file_name.db>")

#     # Passed in arg = sqlite database file
#     database:str = sys.argv[1]

#     # Spark init and data loading
#     # make the s_s reusable without needing a getOrCreate call
#     spark_session = initialize_spark_session()

#     # Debug Lines 
#     # print(f"SPARK VERSION: {spark_session.version}")
#     # print(f"SCALA VERSION: {spark_session._jvm.scala.util.Properties.versionString()}")

#     spark_tables = load_sqlite_tables(spark_session, database)
#     video_relation_graph = network_aggregation(spark_tables[2], spark_tables[3])
#     # degree_reporting(video_relation_graph)
#     graph_and_display(video_relation_graph, "Example Video Relation Network Graph")

# if __name__ == '__main__':
#     main()