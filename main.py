from extract import read_csv_file
from extract import transformation_card_number, transformation_date_time, tranformation, product_tb, order_item_tb, order_tb
from pathlib import Path
import shutil

raw_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/raw_data")
extracted_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/extracted_data")
transformed_data_folder = Path("/Users/ruthfashogbon/Desktop/personal-project/etl-pipeline-project/transformed_data")
# loaded_data_folder

main_menu = ["Extract", "Transform", "Load", "Auto ETL", "Exit App"]

def print_list(list_name):
    for i, option in enumerate(list_name, start=1):
        print (f"{i} - {option}")

def get_csv_filepaths_from(folder_path): # For each file path in the raw_data_folder (or another folder), if it's a file (not a folder) and has a '.csv' extension, add the Path object to the files list.
    files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix == '.csv']
    return files


# ADD A WHILE LOOP - 
print_list(main_menu)
main_menu_selection = input("Enter an option from the main menu (e.g., 1, 2, 3): ")
if main_menu_selection == "1":
    files = get_csv_filepaths_from(raw_data_folder) 
    if not files:
        print("No CSV files found in the raw data folder.")
    else:
        print_list([f.name for f in files]) # Print the names of all CSV files in the files list. (.name -  can use .name because each item in the files list is a Path object from the pathlib module.)

    csv_selection = input("Enter the number of the CSV file you want (e.g., 1, 2, 3):") # User input to select which to extract 
    selected_file = files[int(csv_selection) - 1]   
    df = read_csv_file(selected_file) # read with panda 
    print(df)

    if df is not None: # check if the extraction function worked.
        shutil.move(str(selected_file), str(extracted_data_folder / selected_file.name)) # moving file from raw folder ot extracted. shutil.move(source, destination)
        print(f"{selected_file.name} has been moved to the extracted_data folder.")
    else:
        print(f"{selected_file.name} was not moved due to extraction issues.")

    # Re-scan the folder to update available CSVs  so it knows that the file has moved if it has 
    get_csv_filepaths_from(raw_data_folder) 

# print list of options ("extract another, transform data, exit to main menu")      

# perform transformation and after all done, move to the transformed folder
if main_menu_selection == "2":
    files = get_csv_filepaths_from(extracted_data_folder)
    if not files:
        print("No CSV files found in the raw data folder.")
    else:
        print_list([f.name for f in files]) # Print the names of all CSV files in the files list. (.name -  can use .name because each item in the files list is a Path object from the pathlib module.)

    csv_selection = input("Enter the number of the CSV file you want (e.g., 1, 2, 3):") # User input to select which to extract 
    selected_file = files[int(csv_selection) - 1]  

    df = read_csv_file(selected_file)  # Read selected CSV

    #  Transform function 
    tranformation(df)
    product_tb()
    order_item_tb()
    order_tb()
    print("The files are now ready to be loaded into the database. Please go to the 'load' step to complete this process.")



    
    # extract csv selected  - move to extracted folder  (print csv name has been extracted and is ready to be transformed)

        # if 1 - repeat process - need ot add while loop to add continue 
        # if 2 goes to transformation part
        # if 3 exit while loop back to main menu 


