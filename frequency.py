# report categorized statistics on the frequency of videos partitioned on a certain search condition
# Matthew Gaskell

# built-in
import sys
import sqlite3

def frequency_statistics(db_name: str) -> None:
    database = sqlite3.connect(db_name)
    cur = database.cursor()

    # Statistic options, can add more later 
    u_input_statistic = input(f"Please select the statistic you would like to partition on:" + 
                                f"[1] View Count" + 
                                f"[2] Length" + 
                                f"[3] Video Rating" +
                                f"[4] Number of Comments")
    
    u_input_sign = input(f"Please select the direction of the partition: < or > ")

    # Input validation (avoid injection)
    if not(u_input_sign == ">" or u_input_sign == "<"): 
        print(f"Direction must be in the form of < or >.")

    u_input_value = input(f"Please enter the value on which to partition: ")

    # Input validation (avoid injection)
    try:
        float(u_input_value)
    except ValueError:
        print(f"Please enter a valid number. ")

    # Match to column name in the db table
    column_name = ""
    match u_input_statistic:
        case "1":
            column_name = "views"

        case "2":
            column_name = "length"

        case "3":
            column_name = "rate"

            if u_input_value > 1 or u_input_value < 0:
                print(f"Rating must be between 0.00 and 0.99.")

        case "4":
            column_name = "comments"

    # Query, get value, and print to user
    cur.execute("SELECT COUNT(*) FROM Video WHERE " + column_name + " " + u_input_sign + " " + u_input_value)
    count_value = cur.fetchone()[0]
    print(f"The number of videos with {column_name} {u_input_sign} {u_input_value} is {count_value}.")

# Main (used for debugging/testing when program not ran from main.py, requires existing database file)
def main() -> None:
    # Check for passed in cmdline argument
    if len(sys.argv) != 2:
        print("Usage: python frequency.py <database_file_name.db>")

    # Passed in arg = sqlite database file
    database:str = sys.argv[1]

    frequency_statistics(database)

# Direct File Run
if __name__ == "__main__":
    main()