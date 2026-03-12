from pyspark.sql import SparkSession, DataFrame

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
def load_sqlite_tables(db_name: str):
    category_df = load_table_into_spark(db_name, "Category")
    user_df = load_table_into_spark(db_name, "User")
    video_df = load_table_into_spark(db_name, "Video")
    relation_df = load_table_into_spark(db_name, "Relation")