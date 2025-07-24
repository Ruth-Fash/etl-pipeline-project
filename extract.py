import os
os.environ["DISABLE_PANDERA_IMPORT_WARNING"] = "True"  # Suppress Pandera import warning by setting this environment variable, # BEFORE importing pandera. Pandera checks this variable at import time

import pandas as pd
import uuid
from rapidfuzz import process
import pandera as pa
from pandera.dtypes import DateTime




def get_csv_filepaths_from(folder_path): # For each file path in the raw_data_folder (or another folder), if it's a file (not a folder) and has a '.csv' extension, add the Path object to the files list.
    files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix == '.csv']
    return files

"""Extract:
Imporing raw data csv file - storing data into a dataframe using pandas """
def read_csv_file(csv,dtype=None):
    try:
        df = pd.read_csv(csv,dtype=dtype)
        return df
    except FileNotFoundError:
        print("Oops! The file was not found. Please check the path.")
        return None
    except pd.errors.EmptyDataError:
        print("The file is empty!")
        return None


def read_all_csvs(folder):
    csv_path = get_csv_filepaths_from(folder)
    all_branch_df = []
    for path in csv_path:
         df = pd.read_csv(path)
         all_branch_df.append(df)  # Use filename (without .csv) as key

    combined_df = pd.concat(all_branch_df, ignore_index=True) # Combine all DataFrames into one (resetting index)
    return combined_df


    """ Test to see the output"""
# extract_raw_data('data/unnormalized_orders_with_abnormal_cards.csv')

def rename_columns(df):
    df = df.rename(columns={"Date/Time": "date_time", "Branch": "branch", "Payment Type": "payment_type", "Product": "product", "Price": "price"})
    return df

"""Transform
1. Change data-types
2. Clear white spaces from all coloumns
3. Replace blanks with Null
4. Get rid of any "-" in card number
5. Split out Drink & Price(Float, no £) - How to deal with customer with multiple order 
4. Check Spelling - 
7. Data schema validation frameworks 
8. Creat UUID - name, date/time/card number  """


def order_uuid(df):
    df['order_id'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df
def product_uuid(df):
    df['product_id'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df
def order_item_uuid(df):
    df['order_item_id'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df
# Checks blanks - for data if anything is blank should be missing data as all fields must be populated , so if not replace with unknown
def fix_blanks(df):
    cols = ["date_time", "branch", "payment_type", "product", "price"]
    df[cols] = df[cols].replace("", pd.NA)
    return df

def remove_and_save_blank_rows(df):
    has_missing_values =  df[["product", "price"]].isna().any(axis=1)# check for actual NaN / pd.NA values in the df
    rows_with_missing = df[has_missing_values].copy()  # copy rows with blanks in the df
    
    if not rows_with_missing.empty:
        rows_with_missing.to_csv('rows_with_missing.csv', index=False) # save to csv 
        print("Rows with missing 'Product' or 'Price' have been saved to 'rows_with_missing.csv' and removed from the main dataset.")
        
        return df[~has_missing_values] # return rows without the blanks
        


# Take extracted data that is in tranform folder to transform it 
def transformation_split_orders(df):
    if df.empty:
        print("The DataFrame is empty — no data available to transform.")
        return None
    if "Drinks Ordered" not in df.columns:
        print("Column 'Drinks Ordered' not found in DataFrame.")
        return None
    else:
        try:
            # Step 1: Split the 'drinks_ordered' column on the commas to get lists of drinks
            df["Drinks Ordered"] = df["Drinks Ordered"].str.split(",")
            # Step 2: Explode the list into separate rows - Explode: separates into row, while duplicating the other columns to match.
            df_product = df.explode("Drinks Ordered")
            # Strip any whitespace
            df_product["Drinks Ordered"] = df_product['Drinks Ordered'].str.strip()
            return df_product
        except Exception as e:
            print(f"Error during transformation (splitting orders), please resolve before moving forward  {e}")
            return None

def transformation_date_time(df): 
    if df.empty:
        print("The DataFrame is empty — no data available to transform.") # check if df is empty, i empty return none
        return None
    if "Date/Time" not in df.columns:  # check if date/time column exists, if not raise error
        print("Column 'Date/Time' not found in DataFrame.")
        return None
    else:
        try: # if all good try to concert column to certain format, and return change (df), if soemthing happens raise error 
            df["Date/Time"] = df["Date/Time"].astype(str).str.strip()
            df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="mixed", dayfirst=True)  # Convert those strings into pandas datetime objects , dayfirst = True explicitly telling pandas how to interpret the order.
            return df
        except Exception as e:
            print(f"Error during transformation of Date/Time Column, please resolve before moving forward {e}")
            return None

def transformation_card_number(df): # check column exists
    df = df.drop(columns=["Card Number"]) 
    return df

def transformation_customer_name(df): # check column exists
    df = df.drop(columns=["Customer Name"]) 
    return df  

def transformation_branch(df): # check if coloumn exists, list of branches against what in the data? - message new branch detected?
    if df.empty: 
        print("The DataFrame is empty — no data available to transform.")
        return None
    if "Branch" not in df.columns:
        print("Column 'Branch' not found in DataFrame.")
        return None
    else:
        df["Branch"] = df["Branch"].str.strip().str.capitalize()

        valid_branch_df = read_csv_file("valid_branch_list.csv")["Branch"].dropna().str.strip().tolist() 
        df["Branch"] =  df["Branch"].apply(lambda x: fuzzy_correction(x,valid_branch_df)) 
        return df  
  
def transformation_payment_type(df):
    allowed_payment = ("Card", "Cash")
    if df.empty:
        print("The DataFrame is empty — no data available to transform.") # check if df is empty, i empty return none
        return None
    if "Payment Type" not in df.columns:
        print("Column 'Payment Type' not found in DataFrame.")
        return None      
    df["Payment Type"] = df["Payment Type"].str.strip().str.capitalize()
    return df

# Your fuzzy correction function 
def fuzzy_correction(name, valid_list, threshold=85): # check columns exist,
    # Load your valid drinks/product/branch list from CSV, convert to a plain python list of strings (rapidfuzz expects list of choices not panda series)
    if pd.isna(name):
        return None
    match, score, _ = process.extractOne(name, valid_list)
    return match if score >= threshold else name

def transformation_product_price(df): #check column exist, and there is a price and product name 
    # Define regex pattern to capture product name and price separately
    # ^(.*?)      -> Capture product name (non-greedy)
    # \s*[-]?\s*  -> Optional spaces, optional dash, optional spaces
    # £?          -> Optional £ sign (not captured)
    # ([\d\.]+)   -> Capture price (digits and dot)
    # $           -> End of string
    pattern = r'^(.*?)\s*[-]?\s*£?([\d\.]+)$'
    # Extract 'Product' and 'Price' into separate columns based on pattern
    df[['Product', 'Price']] = df['Drinks Ordered'].str.extract(pattern)
    # Convert the 'Price' column to float type (removes any string formatting)
    df['Price'] = df['Price'].astype(float)
    # Drop the original 'Drinks Ordered' column as it's no longer needed
    df = df.drop(columns=['Drinks Ordered'])

    valid_product_list = pd.read_csv("valid_drinks_list.csv")["Product"].dropna().str.strip().tolist() # read the valid list csv
    df["Product"] =  df["Product"].apply(lambda x: fuzzy_correction(x,valid_product_list)) # Applies the correct_drink_name function to each value (x) in the 'Product' column.
    return df

""""""""""VALIDATING THE DATA SCHEMA"""""""""
no_whitespace = pa.Check(lambda s: s == s.str.strip())
not_blank = pa.Check(lambda s: s.str.strip().str.len() > 0)

def validate_schema(df,schema):
    # This will raise an error if validation fails
    return schema.validate(df)

product_schema = pa.DataFrameSchema({
                            "product":pa.Column(str, 
                                        checks=[no_whitespace, not_blank]
                                        ,nullable=False),
                            "price":pa.Column(float, checks=pa.Check.gt(0), 
                                nullable=False),
                            "product_id":pa.Column(str, checks=[no_whitespace, not_blank], 
                                nullable=False)},
                            unique=["product", "price"],
                            strict=True
                            )

order_item_schema = pa.DataFrameSchema({"quantity":pa.Column(int,
                                            checks=pa.Check.gt(0),
                                            nullable=False),
                                        "product_id":pa.Column(str, 
                                            checks=[no_whitespace, not_blank], 
                                            nullable=False),
                                        "order_id":pa.Column(str, 
                                            checks=[no_whitespace, not_blank], 
                                            nullable=False),
                                        "order_item_id":pa.Column(str, 
                                            checks=[no_whitespace, not_blank], 
                                            nullable=False)},
                                            strict=True)

order_schema = pa.DataFrameSchema({"order_id":pa.Column(str, 
                                            checks=[no_whitespace, not_blank], 
                                            nullable=False),
                                    "date_time":pa.Column(DateTime, nullable=False),
                                    "branch":pa.Column(str, checks=[no_whitespace, not_blank], nullable=False),
                                    "payment_type":pa.Column(str, nullable=True)})

#Transform all on one tabel first
def transformation(df):
    # List of transformation functions to apply, in order
    funcs = [
        order_uuid,
        transformation_split_orders,
        transformation_branch,
        transformation_payment_type,
        transformation_date_time,
        transformation_card_number,
        transformation_customer_name,
        transformation_product_price,
        rename_columns,
        fix_blanks,
        remove_and_save_blank_rows
    ]
    # Loop over each function in the list
    for func in funcs:
        # Call the current function with the dataframe and update df with the result
        df = func(df)
        # Check if the function returned None, indicating an error or failure
        if df is None:
            # Print which transformation failed and stop processing
            print(f"Transformation failed at {func.__name__}. Stopping process.")
            return None
        
    # Save the fully transformed DataFrame to CSV
    df.to_csv("extracted_data/unnormalised_data.csv", index=False)
    return df

 # Seperate onto product table and creat product uuid
def product_tb(folder_name):
    df = read_csv_file("extracted_data/unnormalised_data.csv")
    product_df = df[["product", "price"]].drop_duplicates().copy()
    product_df = product_uuid(product_df)  # Add UUIDs directly to the product_df
    try:
        validated_product_df = validate_schema(product_df,product_schema) # check data vlaidation of product table columns
    except pa.errors.SchemaError as e:
        print("Products Table Validation failed:", e)
        return None # Stop the function if validation fails
    except Exception as e:
        print("Unexpected error during product schema validation:", e)
        return None
    validated_product_df.to_csv(f"transformed_data/{folder_name}/products_table.csv", index=False) # Save the unique product-price pairs to a csv from the DF
    print("Product Table saved successfully!")
    return validated_product_df

def order_item_tb(folder_name):
    df = read_csv_file("extracted_data/unnormalised_data.csv")
    order_item_df = df[["order_id", "product" ]].copy()
    order_item_df.to_csv(f"transformed_data/{folder_name}/order_item_table.csv", index=False) 

    # Merge order_item[prouduct] with product[product] and include  product id 
    product_df = pd.read_csv(f"transformed_data/{folder_name}/products_table.csv")
    order_item_df = order_item_df.merge(product_df[["product","product_id"]], on="product", how="left") # Merge 'Product ID' from product_df into order_item_df based on matching 'Product' names
    order_item_df = order_item_df.groupby(["order_id", "product_id"]).size().reset_index(name="quantity") # group the two coloumsn and count how many times a combo appear, reset name to 
    order_item_df = order_item_uuid(order_item_df)  # Add UUIDs directly to the product_df
    try:
        validated_order_item_df = validate_schema(order_item_df, order_item_schema )
    except pa.errors.SchemaError as e:
        print("Order Items Table Validation failed:", e)
        return None # Stop the function if validation fails
    except Exception as e:
        print("Unexpected error during product schema validation:", e)
        return None

    validated_order_item_df.to_csv(f"transformed_data/{folder_name}/order_item_table.csv", index=False) # Save again with UUIDs and merge
    print("Order Items Table saved successfully!")
    return validated_order_item_df

# Create a seperat order table - where we delete coloumns not needed
def order_tb(folder_name):
    df = pd.read_csv("extracted_data/unnormalised_data.csv")
    order_df = df[["order_id", "date_time", "branch", "payment_type"]].copy()
    order_df["date_time"] = pd.to_datetime(order_df["date_time"].astype(str).str.strip(), errors='raise')
    try:
        validated_order_df = validate_schema(order_df, order_schema)
    except pa.errors.SchemaError as e:
        print("Order Table Validation failed:", e)
        return None
    except Exception as e:
        print("Unexpected error during product schema validation:", e)
        return None

    validated_order_df.to_csv(f"transformed_data/{folder_name}/order_table.csv", index=False) # save the new df to a csv
    print("Order Table saved successfully!")
    return validated_order_df



# check before goign aheadf with tranformation that in contains columns expected
# need to find a way to chnage the name of the file unique to the file itself. for instance could have 2 files from different dates that neeed etl process but need to move from extract to to tranform , under their same name just added transformed at the end 
# archive the files once transformation/ load process done 
# need ot chnage data to be for 1 day only - naming convention with dataa ein front of it and time
# saves as it's current file name, so for instance file name may be current extracted_2024_07_09_camden.csv, and once the transformation phase is done I want to save the data frame under an altered name like unormalised_2024_07_09_camden.csv
# for fuzzy check with branches, i need to flag if the branch name exists in the branch list check csv. 



#  .copy() is used to explicitly create a new, independent copy of a DataFrame (or a slice of one) so that:
# You're working on the copy, not just a view (or reference) of the original.
# It avoids confusing side effects or warnings like SettingWithCopyWarning.
# Your transformations are safe and won't accidentally modify the original data or depend on it.
