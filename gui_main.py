from extract import read_csv_file, read_all_csvs
from extract import transformation_card_number, transformation_date_time, transformation, product_tb, order_item_tb, order_tb, product_schema, order_item_schema, order_schema
from load_data.sql_connection import load_to_database
from pathlib import Path
import shutil
import os 
import sys
import pandas as pd
import time
import customtkinter as ctk
import traceback
from tkinter import messagebox


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

fg_color="darkblue"

def print_list(list_name):
    for i, option in enumerate(list_name, start=1):
        print (f"{i} - {option}")

def get_subfolders(folder_path):
    folders = [f for f in folder_path.iterdir() if f.is_dir() and f.name != "archive" ]
    return folders

def get_csv_filepaths_from(folder_path): # For each file path in the raw_data_folder (or another folder), if it's a file (not a folder) and has a '.csv' extension, add the Path object to the files list.
    files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix == '.csv']
    return files

def print_title(menu_name):
    print(f"\n===== {menu_name.upper()} =====\n")

def clear_window(window):
    for widget in window.winfo_children():
        widget.destroy()

def move_folder(selected_folder, status_label, transformation_button):
    global show_transform_button  # Add this line to modify the global variable
    df = read_all_csvs(selected_folder) # read with panda puts into a df

    if df is not None: # check if the extraction function worked.
        shutil.copytree(str(selected_folder), str(extracted_data_folder / f"extracted_{selected_folder.name}")) # copy file from raw folder to extracted folder under new name with extracted at front. shutil.move(source, destination)
        shutil.move(str(selected_folder), str(archive_raw_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder 
        messagebox.showinfo("Success", f"{selected_folder.name} moved, and are now ready to be transformed. Click 'Transform' to complete the tranformation process.")
        show_transform_button = True
        menu_selection_1()

    else:
        messagebox.showinfo(f"{selected_folder.name} not moved due to extraction issues.")
  
def scrollable_frame():
    scrollable_frame = ctk.CTkScrollableFrame(app, height=400,)
    scrollable_frame.pack(padx=20, pady=20, fill="both", expand=True)
    return scrollable_frame


user_entry = None
def tranformation(selected_folder, status_label, load_button, parent_frame):
    global user_entry

    df = read_all_csvs(selected_folder)  # Read all CSVs in the selected folder into one DataFrame
    if df is None:   # Check if reading failed (returns None)
         status_label.configure(text=f"{selected_folder.name} files are empty. Transformation is not possible .")

    status_label.configure(text="Enter the sales date to create new folder (e.g.,20240625 (yyyymmdd)): ")

   # Check if the entry box exists and is visible
    if not (user_entry and user_entry.winfo_exists() and user_entry.winfo_ismapped()):  # if user entry has value assigned to it and the widget object exists, if true goes to next if, if not true moves to creating the widget
        user_entry = ctk.CTkEntry(parent_frame)
        user_entry.pack(pady=5)

        submit_btn = ctk.CTkButton(
        parent_frame,
        text="Submit",
        command=lambda: on_entry(user_entry, df, selected_folder, status_label, load_button)
    )
        submit_btn.pack(pady=10) # Create Submit button

    else:
        app.update_idletasks() # refresh ui
        return
    
def on_entry(user_entry, df, selected_folder, status_label, load_button):
    global show_load_button

    folder_name = user_entry.get() # get user input from the entry
    if not folder_name.isdigit() or len(folder_name) != 8:
        status_label.configure(text="Invalid date format. Please use YYYYMMDD.", font=("Arial", 16, "bold"))

    new_folder_path = os.path.join("transformed_data", f"{folder_name}")
    os.makedirs(new_folder_path, exist_ok=True)
    status_label.configure(text= f"Folder successfully created at: {new_folder_path}", font=("Arial", 16, "bold"))

    #  Transform function 
    df_transformed = transformation(df, folder_name)
    if df_transformed is None:
        status_label.configure(text= "Transformation failed, stopping further processing.", font=("Arial", 16, "bold"))
        return

    product_df = product_tb(folder_name)
    if product_df is None:
        label = ctk.CTkLabel(app, text= "Transformation failed with product table, stopping further processing.", font=("Arial", 16, "bold"))     
        label.pack(pady=5)
        return 

    order_item_df = order_item_tb(folder_name)
    if order_item_df is None:
        label = ctk.CTkLabel(app, text= "Transformation failed with product table, stopping further processing.", font=("Arial", 16, "bold"))     
        label.pack(pady=5)
        return 

    order_df = order_tb(folder_name)
    if order_df is None:
        label = ctk.CTkLabel(app, text= "Transformation failed with product table, stopping further processing.", font=("Arial", 16, "bold"))     
        label.pack(pady=5)
        return

        """ Ideally you want to say here if all the steps are completed without error it will move the extracted file into the archive, may need to be done in the extract.py"""
    label = ctk.CTkLabel(app, text= "The files are now ready to be loaded into the database. Please go to the 'load' step to complete this process.", font=("Arial", 16, "bold"))
    label.pack(pady=5)


    # move extracted folder to archive once tranformation is done 
    shutil.move(str(selected_folder), str(archive_extracted_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder 
    messagebox.showinfo("Success", "Transformation complete. Click 'Load' to continue.")
    show_load_button = True
    # Refresh the screen to remove the moved folder
    menu_selection_2()


def load(selected_folder, status_label):
    try:
        # df = read_csv_file(selected_folder)  # Read selected CSV, puts into a df
        product_df = read_csv_file(selected_folder / "products_table.csv")
        product_schema.validate(product_df, lazy=True)  # lazy=True collects all errors instead of stopping at first

        order_item_df = read_csv_file(selected_folder / "order_item_table.csv")
        order_item_schema.validate(order_item_df)

        order_df = pd.read_csv(selected_folder / "order_table.csv", parse_dates=["date_time"] )
        order_schema.validate(order_df)

        load_database = load_to_database(product_df, order_df, order_item_df)

        # move tranformed folder to archive once load is completed
        if load_database:
            shutil.move(str(selected_folder), str(archive_transformed_data_folder / f"archive_{selected_folder.name}")) # moves csv in raw data folder to archive file in raw data folder     
            messagebox.showinfo("Success", "Data loaded and archived successfully!")      
            menu_selection_3()
    
        else:
            messagebox.showerror("Error", "Upload to database has failed.")
    except Exception as e:
        messagebox.showerror("Exception", f"Error during load: {str(e)}" )       
        traceback.print_exc()

# Extract
show_transform_button = False
def menu_selection_1():
    clear_window(app)
    app.geometry("500x400")
    app.title("Extract")

    folders = get_subfolders(raw_data_folder) 
    if not folders:
        label = ctk.CTkLabel(app, text="No folders found. Go back to the main menu. or 'Transform' if all extraction has been completed", font=("Arial", 16, "bold"))
        label.pack(pady=10)

        button = ctk.CTkButton(app, text="Main Menu", command=main_page, fg_color=fg_color)
        button.pack(pady=10, padx=20)

        transform_button = ctk.CTkButton(top_frame, text="Transform", command=menu_selection_2)
        transform_button.pack(pady=10, padx=20)
              
    else:
        # === Top button area ===
        top_frame = ctk.CTkFrame(app)
        top_frame.pack(fill="x", pady=(20, 10))

        main_menu_button = ctk.CTkButton(top_frame, text="Main Menu", command=main_page, fg_color=fg_color)
        main_menu_button.pack(side="right", padx=20)

        # === Create the transform button but don't pack it yet ===
        transform_button = ctk.CTkButton(top_frame, text="Transform", command=menu_selection_2)

        # Pack transform_button only if the flag is True
        if show_transform_button:
            transform_button.pack(side="left", padx=20, pady=10)

        status_label = ctk.CTkLabel(app, text="Select a folder to extract", font=("Arial", 16, "bold"))
        status_label.pack(pady=10)

        # === Scrollable Frame with Folder Buttons ===
        scroll = scrollable_frame() 
        for f in folders:
        # Use lambda to delay calling the function until the button is clicked
            button = ctk.CTkButton(
                scroll,
                text=f.name,
                command=lambda folder=f: move_folder(folder, status_label, transform_button),
                fg_color=fg_color
                
            )
            button.pack(pady=10, padx=20)


# Transformation
show_load_button = False
def menu_selection_2():

    clear_window(app)
    app.geometry("500x400")
    app.title("Transformation")
    folders = get_subfolders(extracted_data_folder)

    if not folders:
        label = ctk.CTkLabel(app, text="No folders found. Go back to the main menu or 'Load' if transformation has been completed", font=("Arial", 16, "bold"))
        label.pack(pady=10)

        button = ctk.CTkButton(app, text="Main Menu", command=main_page, fg_color=fg_color)
        button.pack(pady=10, padx=20)

        load_button = ctk.CTkButton(app, text="Load", command=menu_selection_3)
        load_button.pack(pady=10, padx=20)
    else:
            # === Top button area ===
        top_frame = ctk.CTkFrame(app)
        top_frame.pack(fill="x", pady=(20, 10))

        button = ctk.CTkButton(top_frame, text="Main Menu", command=main_page, fg_color=fg_color)
        button.pack(side="right", padx=20)

        # === Create the transform button but don't pack it yet ===
        load_button = ctk.CTkButton(top_frame, text="Load", command=menu_selection_3)

        if show_load_button:
            load_button.pack(side="left", padx=20, pady=10)

        status_label = ctk.CTkLabel(app, text="Select a folder to transform", font=("Arial", 16, "bold"))
        status_label.pack(pady=10)

        scroll = scrollable_frame()
        for f in folders: # Get all subfolders from the extracted data folder
            subfolders =ctk.CTkButton(scroll, text=f.name, command=lambda folder=f: tranformation(folder, status_label, load_button, scroll, ), fg_color=fg_color)
            subfolders.pack(pady=10, padx=20)
    
            

   
# Load
def menu_selection_3():
    clear_window(app)
    app.geometry("500x400")
    app.title("Load")

    button = ctk.CTkButton(app, text="Main Menu", command=main_page, fg_color=fg_color)
    button.pack(side="bottom", pady=10)


    folders = get_subfolders(transformed_data_folder) 
    if not folders:
        label = ctk.CTkLabel(app, text="No folders found. Go back to the main menu", font=("Arial", 16, "bold"))
        label.pack(pady=10)

        button = ctk.CTkButton(app, text="Main Menu", command=main_page, fg_color=fg_color)
        button.pack(pady=10, padx=20)
    else:
        
        status_label = ctk.CTkLabel(app, text="Choose a folder to load to the database:", font=("Arial", 16, "bold"))
        status_label.pack(pady=10)

        scroll = scrollable_frame() 

        for f in folders:
            subfolders = ctk.CTkButton(scroll, text=f.name, command=lambda folder=f: load(folder,status_label), fg_color=fg_color) # folder = f hold what the user selected 
            subfolders.pack(pady=10, padx=20)
    
 
main_menu_function = {"Extract": menu_selection_1, "Transform": menu_selection_2, "Load": menu_selection_3, "Exit": sys.exit}
app = ctk.CTk()

def main_page():
    clear_window(app)
    app.geometry("500x400")
    app.title("ETL Main Menu")
    label = ctk.CTkLabel(app, text="Welcome! Choose a task below", font=("Arial", 16, "bold"))
    label.pack(pady=10)


    scroll = scrollable_frame() 
    for key, value in main_menu_function.items():
        button = ctk.CTkButton(scroll, text=key, fg_color=fg_color, command=value)
        button.pack(pady=10, padx=20)
    # app.update_idletasks() - use this to Let Tkinter calculate new size after widgets are packed, so auto sizes screen based off widgets above 



ctk.set_appearance_mode("System")
main_page()
app.mainloop()  # keeps the window running , without app will just flash and exit instantly



# error if folder exists
# check to see if dates already exist in database, if it does, do not add with load phase 
# get user to auto connect to docker and different contaienrs or check/remind user automatially