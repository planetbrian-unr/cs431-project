# unified file
# Brian Wu, Matthew Gaskell

# built-in
import sys
import shutil

# local
import stages.etl as etl
import stages.spark as spark
import stages.frequency as frequency

def main() -> None:
    # error-catching, must specify arguments
    if len(sys.argv) != 3:
        print("Usage: python main.py <dataset_id> <db_name>")
        return

    # store command-line arguments, and create a list of desired files
    dataset_id:str = sys.argv[1]
    db_name:str = sys.argv[2]
    files:list[str] = [
        f"{dataset_id}/master.csv",
        f"{dataset_id}/users.csv",
        f"{dataset_id}/categories.csv",
        f"{dataset_id}/videos.csv",
        f"{dataset_id}/{db_name}"
    ]

    # unzip dataset files, get depth, and etl
    etl.fetch_dataset(dataset_id)
    depth:int = etl.get_dataset_depth(dataset_id)
    etl.extract(dataset_id, depth, files[0])
    etl.transform(files)
    etl.load(files)

    # move database to main.py's directory and start graph creation with spark
    shutil.copy(files[4], db_name)
    spark_session = spark.initialize_spark_session()
    video_relation_graph = spark.graph_generator(spark_session, db_name)

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
        elif u_input == "e":
            break

        else:
            print("Invalid choice. Please try again.")
    
    return

if __name__ == '__main__':
    main()