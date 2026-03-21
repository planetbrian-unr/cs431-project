# unified file
# Brian Wu, Matthew Gaskell

# built-in
import os
import sys

# local
import etl
import spark

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python main.py <dataset_id> <db_name>")
        return

    # store command-line arguments
    dataset_id:str = sys.argv[1]
    db_name:str = sys.argv[2]

    # establish list of desired files
    files:list[str] = [
        f"{dataset_id}/master.csv",
        f"{dataset_id}/users.csv",
        f"{dataset_id}/categories.csv",
        f"{dataset_id}/videos.csv",
        f"{dataset_id}/{db_name}"
    ]

    # unzip dataset files and get depth
    etl.fetch_dataset(dataset_id)
    depth:int = etl.get_dataset_depth(dataset_id)

    # etl
    etl.extract(dataset_id, depth, files[0])
    etl.transform(files)
    etl.load(files)

    while not os.path.isfile(db_name):
        u_input = input(f"Please move {db_name} from the {dataset_id} directory to where main.py is. " +
                        f"Please click enter when done, or type exit to exit the script\n")
        if u_input == "exit":
            return

    # Spark init and data loading
    # make the s_s reusable without needing a getOrCreate call
    spark_session = spark.initialize_spark_session()
    spark_tables = spark.load_sqlite_tables(spark_session, db_name)
    video_relation_graph = spark.network_aggregation(spark_tables[2], spark_tables[3])
    spark.degree_reporting(video_relation_graph)
    # graph_and_display(agg_graph, "Title")

    

if __name__ == '__main__':
    main()