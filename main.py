# unified file
# Brian Wu, Matthew Gaskell

# built-in
import sys
import shutil

# local
import etl
import spark
import frequency

def graph_generator(db_name):
    with spark.initialize_spark_session() as spark_session:
        # Spark init and data loading
        # make the s_s reusable without needing a getOrCreate call
        spark_tables = spark.load_sqlite_tables(spark_session, db_name)
        video_relation_graph = spark.network_aggregation(spark_tables[2], spark_tables[3])

        return video_relation_graph

def main() -> None:
    # error-catching, must specify arguments
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

    # move database to main.py's directory and start program
    shutil.copy(files[4], db_name)

    # compute a graph to use
    video_relation_graph = graph_generator(db_name)

    # loop menu until exited
    while True:
        u_input:str = input(
            "What would you like to do?\n" +
            "[1] Degree Distribution\n" +
            "[2] Generate a simple Network graph\n" +
            "[3] Categorized Statistics\n" +
            "[e] Exit\n"
        )

        if u_input == "1":
            spark.degree_reporting(video_relation_graph)
    
        elif u_input == "2":
            spark.graph_and_display(video_relation_graph, "Simple YouTube network graph")

        elif u_input == "3":
            frequency.frequency_statistics(db_name)

        # Exit
        if u_input == "e":
            break

        else:
            print("Invalid choice. Please try again.")
    
    return

if __name__ == '__main__':
    main()