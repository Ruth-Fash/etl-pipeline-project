import pandas as pd
import uuid
from rapidfuzz import process


"""Extract:
Imporing raw data csv file - storing data into a dataframe using pandas """
def read_csv_file(csv):
    try:
        df = pd.read_csv(csv)
        return df
    except FileNotFoundError:
        print("Oops! The file was not found. Please check the path.")
        return None
    except pd.errors.EmptyDataError:
        print("The file is empty!")
        return None


""" Test to see the output"""
# extract_raw_data('data/unnormalized_orders_with_abnormal_cards.csv')


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
    df['Order ID'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df
def product_uuid(df):
    df['Product ID'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df
def order_item_uuid(df):
    df['Order Item ID'] = [str(uuid.uuid4()) for i in range(len(df))] # create a uuid  for each cell between 0 and the number of rows in the df
    return df

# Take extracted data that is in tranform folder to transform it 
def transformation_split_orders(df):
    # Step 1: Split the 'drinks_ordered' column on the commas to get lists of drinks
    df["Drinks Ordered"] = df["Drinks Ordered"].str.split(",")

    # Step 2: Explode the list into separate rows - Explode: separates into row, while duplicating the other columns to match.
    df_product = df.explode("Drinks Ordered")

    # Strip any whitesapce
    df_product["Drinks Ordered"] = df_product['Drinks Ordered'].str.strip()
    return df_product

def transformation_date_time(df):
    df["Date/Time"] = df["Date/Time"].astype(str).str.strip()
    df["Date/Time"] = pd.to_datetime(df["Date/Time"], format="mixed", dayfirst=True)  # Convert those strings into pandas datetime objects , dayfirst = True explicitly telling pandas how to interpret the order.
    return df

def transformation_card_number(df):
    df = df.drop(columns=["Card Number"])  # drop card number but after establishing uuid 
    return df

def tranformation_customer_name(df):
    df = df.drop(columns=["Customer Name"])
    return df  

def transformation_branch(df):
    df["Branch"] = df["Branch"].str.strip()
    return df  
    # check against all store names

def transformation_payment_type(df):
    df["Payment Type"] = df["Payment Type"].str.strip()
    return df
    # only cash or card

# Your fuzzy correction function
def correct_drink_name(drink_name, valid_list, threshold=85):
    # Load your valid drinks list from CSV, convert to a plain python list of strings (rapidfuzz expects list of choices not panda series)
    if pd.isna(drink_name):
        return None
    match, score, _ = process.extractOne(drink_name, valid_list)
    return match if score >= threshold else drink_name


def transformation_product_price(df):
    # Split the drink name and price into separate columns - everything once space after the drink name, split into a seperate coloumn 
    df[["Product", "Price"]] = df["Drinks Ordered"].str.split(' - ', n=1, expand=True)

    # Convert price to float and replace £ with ""
    df["Price"] = df["Price"].str.replace("£", "", regex=False).astype(float)

    # Drop the old column you don't need
    df = df.drop(columns=['Drinks Ordered'])
    return df

    # if df["product"] == product_tb["product"]:
    #     df["product"].replace(with corrsponding uuid for that opeodct in the product tble)
    
#Transform all on one tabel first
def tranformation(df):
    df = order_uuid(df)
    df = transformation_split_orders(df)
    df = transformation_branch(df)
    df = transformation_payment_type(df)
    df = transformation_date_time(df)
    df = transformation_card_number(df)
    df = tranformation_customer_name(df)
    df = transformation_product_price(df)
    
    valid_list = pd.read_csv("valid_drinks_list.csv")["Product"].dropna().str.strip().tolist()
    df["Product"] =  df["Product"].apply(lambda x: correct_drink_name(x,valid_list)) # Applies the correct_drink_name function to each value (x) in the 'Product' column.
    # It compares x against the list of valid drinks in valid_list using fuzzy matching.
    # If a close enough match is found (above threshold), it replaces x with the corrected name

    df.to_csv("extracted_data/transformed_data.csv", index=False)

 # Seperate onto product table and creat product uuid
def product_tb():
    df = read_csv_file("extracted_data/transformed_data.csv")
    product_df = df[["Product", "Price"]].drop_duplicates().copy()
    product_df.to_csv("extracted_data/products_transformed.csv", index=False) # Save the unique product-price pairs
    product_df = product_uuid(product_df)  # Add UUIDs directly to the product_df
    product_df.to_csv("extracted_data/products_transformed.csv", index=False) # Save again with UUIDs

def order_item_tb():
    df = read_csv_file("extracted_data/transformed_data.csv")
    order_item_df = df[["Order ID", "Product" ]].copy()
    order_item_df.to_csv("extracted_data/order_item_transformed.csv", index=False) # Save the unique product-price pairs
    order_item_df = order_item_uuid(order_item_df)  # Add UUIDs directly to the product_df
    
    # Merge order_item[prouduct] with product[product] and include  product id 
    product_df = pd.read_csv("extracted_data/products_transformed.csv")
    order_item_df = order_item_df.merge(product_df[["Product", "Product ID"]], on="Product", how="left")

    order_item_df.to_csv("extracted_data/order_item_transformed.csv", index=False) # Save again with UUIDs and merge

    # drop product from order item, drop necessary columns from order table 
    # if order_id and product the same, group so quantity increases (need to still creat quantity column)
    # neeed to rename file for first one to order , so we ahve order, product, order item 
    # ideally these 3 csvs should move to the transformed folder once done 




#  .copy() is used to explicitly create a new, independent copy of a DataFrame (or a slice of one) so that:
# You're working on the copy, not just a view (or reference) of the original.
# It avoids confusing side effects or warnings like SettingWithCopyWarning.
# Your transformations are safe and won't accidentally modify the original data or depend on it.
