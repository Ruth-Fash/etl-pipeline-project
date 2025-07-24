from extract import read_csv_file, read_all_csvs
from extract import transformation_card_number, transformation_date_time, transformation, product_tb, order_item_tb, order_tb, product_schema, order_item_schema, order_schema
from load_data.sql_connection import load_to_database
from pathlib import Path
import shutil
import os 
import sys
import pandas as pd

def clear_terminal():
    # For Windows, the command is 'cls'
    # For Mac/Linux, the command is 'clear'
    os.system('cls' if os.name == 'nt' else 'clear')

raw_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/raw_data")
archive_raw_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/raw_data/archive")
extracted_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/extracted_data")
archive_extracted_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/extracted_data/archive")
transformed_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/transformed_data")
archive_transformed_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/transformed_data/archive")
# loaded_data_folder

main_menu = ["Extract", "Transform", "Load", "Auto ETL", "Exit App"]

def print_list(list_name):
    for i, option in enumerate(list_name, start=1):
        print (f"{i} - {option}")

def get_subfolders(folder_path):
    folders = [f for f in folder_path.iterdir() if f.is_dir()]
    return folders


def get_csv_filepaths_from(folder_path): # For each file path in the raw_data_folder (or another folder), if it's a file (not a folder) and has a '.csv' extension, add the Path object to the files list.
    files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix == '.csv']
    return files


while True:
    clear_terminal()
    print_list(main_menu)
    main_menu_selection = input("Enter an option from the main menu (e.g., 1, 2, 3): ")
    if main_menu_selection == "1":
        folders = get_subfolders(raw_data_folder) 
        if not folders:
            print("No folders found in the raw data folder.")
            break
        else:
            print_list([f.name for f in folders]) # Print the names of all CSV files in the files list. (.name -  can use .name because each item in the files list is a Path object from the pathlib module.)

        folder_selection = input("Enter the number of the folder to process (e.g., 1, 2, 3):") # User input to select which to extract 
        selected_folder = folders[int(folder_selection) - 1]   
        df = read_all_csvs(selected_folder) # read with panda puts into a df
        print(df)

        if df is not None: # check if the extraction function worked.
            shutil.copytree(str(selected_folder), str(extracted_data_folder / f"extracted_{selected_folder.name}")) # copy file from raw folder to extracted folder under new name with extracted at front. shutil.move(source, destination)
            shutil.move(str(selected_folder), str(archive_raw_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder 
            print(f"{selected_folder.name} has been moved to the extracted_data folder.")
        else:
            print(f"{selected_folder.name} was not moved due to extraction issues.")

        # Re-scan the folder to update available CSVs  so it knows that the file has moved if it has 
        get_subfolders(raw_data_folder)

        next_step = input("Enter 1 to move to the tranformation step, or 0 to exit to the main menu")
        if next_step == "1":
            print("under construction")
            break

        elif next_step == "0":
            continue


        
    # print list of options ("extract another, transform data, exit to main menu")      

    # perform transformation and after all done, move to the transformed folder
    if main_menu_selection == "2":
        folder = get_subfolders(extracted_data_folder) # Get all subfolders from the extracted data folder
        if not folder:
            print("No folder found in the raw data folder.")
        else:
            print_list([f.name for f in folder]) # Show the list of folder names to the user

        csv_selection = input("Enter the number of the folder you want (e.g., 1, 2, 3): ") # User input to select which to extract 
        selected_folder = folder[int(csv_selection) - 1]  

        df = read_all_csvs(selected_folder)  # Read all CSVs in the selected folder into one DataFrame

        if df is None:   # Check if reading failed (returns None)
            print("File read failed, exiting.")
            sys.exit(1)

        # Creating folder to store transformed data
        parent_folder = "transformed_data"
        folder_name = input("Enter the sales date (e.g.,20240625 (yyyymmdd)):")
        new_folder_path = os.path.join(parent_folder, f"{folder_name}")
        os.makedirs(new_folder_path, exist_ok=True)
        print(f"Folder successfully created at: {new_folder_path}")


        #  Transform function 
        df_transformed = transformation(df)
        if df_transformed is None:
            print("Transformation failed, stopping further processing.")
            sys.exit(1)

        product_df = product_tb(folder_name)
        if product_df is None:
            print("Transformation failed with product table, stopping further processing.")
            sys.exit(1)

        order_item_df = order_item_tb(folder_name)
        if order_item_df is None:
            print("Transformation failed with order item table, stopping further processing.")
            sys.exit(1)

        order_df = order_tb(folder_name)
        if order_df is None:
            print("Transformation failed with order table, stopping further processing.")
            sys.exit(1)
            """ Ideally you want to say here if all the steps are completed without error it will move the extracted file into the archive, may need to be done in the extract.py"""
        print("The files are now ready to be loaded into the database. Please go to the 'load' step to complete this process.")
        
        # move extracted folder to archive once tranformation is done 
        shutil.move(str(selected_folder), str(archive_extracted_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder 
        print(f"{selected_folder.name} has been moved to the archive folder.")


        next_step = input("Enter 1 to move to the Loading step, or 0 to Exit to the main menu")
        if next_step == "1":
            print("under construction")
            break

        elif next_step == "0":
            continue


    if main_menu_selection == "3":
        folders = get_subfolders(transformed_data_folder) 
        if not folders:
            print("No CSV files found in the raw data folder.")
        else:
            print_list([f.name for f in folders]) # Print the names of all CSV files in the files list. (.name -  can use .name because each item in the files list is a Path object from the pathlib module.)
            folder_selection = input("Enter the number of the folder you want (e.g., 1, 2, 3): ") # User input to select which to extract 
            selected_folder = folders[int(folder_selection) - 1]  

            
            # df = read_csv_file(selected_folder)  # Read selected CSV, puts into a df
            product_df = read_csv_file(selected_folder / "products_table.csv")
            product_schema.validate(product_df, lazy=True)  # lazy=True collects all errors instead of stopping at first
            print(product_df)


            order_item_df = read_csv_file(selected_folder / "order_item_table.csv")
            order_item_schema.validate(order_item_df)
            print(order_item_df)

            order_df = pd.read_csv(selected_folder / "order_table.csv", parse_dates=["date_time"] )
            order_schema.validate(order_df)
            print(order_df)

            load_to_database(product_df, order_item_df, order_df)
            print("Data has been loaded to database")

            # move tranformed folder to archive once load is completed 
            shutil.move(str(selected_folder), str(archive_transformed_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder 
            print(f"{selected_folder.name} has been moved to the archive folder.")

            input("\nPress Enter to go back to the main menu...")

    if main_menu_selection == "5":
        sys.exit()





# need to wrap each stage in a fucntion , in order to do menu == 4 , but in order do allow entry for next steps == 2


