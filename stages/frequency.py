# report categorized statistics on the frequency of videos partitioned on a certain search condition
# Matthew Gaskell, Brian Wu

# built-in
# import sys
import sqlite3

# menu
def frequency_statistics(db_name:str) -> None:
    while True:
        # create dictionary
        column_name_dict:dict[str, str] = {
            "1": "views",
            "2": "length",
            "3": "rate",
            "4": "comments"
        }

        u_input_statistic:str = input(
            "Please select the statistic you would like to partition on:\n" + 
            "[1] View Count\n" + 
            "[2] Length\n" + 
            "[3] Video Rating\n" +
            "[4] Number of Comments\n" +
            "[r] Return to main menu\n"
        )
        
        if u_input_statistic in ("1", "2", "3", "4"):
            column_name:str = column_name_dict[u_input_statistic]
            u_input_sign:str = return_comparator()
            u_input_value:float = return_value(int(u_input_statistic))

            # sql query
            return_statistic(db_name, column_name, u_input_sign, u_input_value)

        # Exit
        elif u_input_statistic == "r":
            break

        else:
            print("Invalid choice. Please try again.")

# loop until valid comparison sign is inputted
def return_comparator() -> str:
    u_input_sign:str = ""
    while True:
        u_input_sign = input(f"Please select the direction of the partition (< or >): ")

        # Input validation (avoid injection)
        if not(u_input_sign == ">" or u_input_sign == "<"): 
            print(f"Direction must be in the form of < or >.\n")
        else:
            break

    return u_input_sign

# loop until valid int/float is given
def return_value(u_input_statistic:int) -> float:
    u_input_value:str = ""
    while True:
        u_input_value = input(f"Please enter the value on which to partition: ")

        # Input validation (avoid injection)
        try:
            float(u_input_value)
        except ValueError:
            print(f"Please enter a valid number.\n")
            continue
        # stage 2: checks the validity of the rating value
        if u_input_statistic == 3 and not (0 <= float(u_input_value) < 1):
            print(f"Rating must be between 0.00 and 0.99.")
            continue
        break

    # an int is a float. removes leading decimal
    return float(u_input_value) if u_input_statistic == 3 else int(u_input_value)

# Takes in user input for search conditions and then queries the SQLite database
def return_statistic(db_name:str, col_name:str, comparator:str, value:float) -> None:
    # automatic connection management
    with sqlite3.connect(db_name) as con:
        cur:sqlite3.Cursor = con.cursor()
        cur.execute(f"""
                    SELECT COUNT(*)
                    FROM Video
                    WHERE {col_name} {comparator} {value};
                    """)
        print(f"The number of videos with {col_name} {comparator} {value} is {cur.fetchone()[0]}.\n")

# Main (used for debugging/testing when program not ran from main.py, requires existing database file)
# def main() -> None:
#     # Check for passed in cmdline argument
#     if len(sys.argv) != 2:
#         print("Usage: python frequency.py <database_file_name.db>")

#     # Passed in arg = sqlite database file
#     database:str = sys.argv[1]

#     frequency_statistics(database)

# if __name__ == "__main__":
#     main()