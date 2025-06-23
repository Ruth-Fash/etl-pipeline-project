import pandas as pd
import uuid


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


# check spelling - product, branch, payment type 
#remianing white space 
#create white space 




#Transform all on one tabel first
def tranformation(df):
    df = order_uuid(df)

    # Split the 'drinks_ordered' column on the commas to get lists of drinks
    df = transformation_split_orders(df)
    df = transformation_date_time(df)
    df = transformation_card_number(df)
    df = tranformation_customer_name(df)

    # split to seperate tables 

    # Split the drink name and price into separate columns - everything once space after the drink name, split into a seperate coloumn 
    df[["Product", "Price"]] = df["Drinks Ordered"].str.split(' - ', n=1, expand=True)


    # Convert price to float and replace £ with ""
    df["Price"] = df["Price"].str.replace("£", "", regex=False).astype(float)

    # Drop the old column you don't need
    df = df.drop(columns=['Drinks Ordered'])

    # Seperate onto product table 

    # fill

    # take order item and quanity and group - remove duplicates but group qty

    df.to_csv("extracted_data/transformed_data.csv", index=False)





    # Add Products/Orders/Order_Item to its own dataframe and then save to seperate csv 
    # orders = df[['Order ID', 'Date/Time', 'Branch', 'Payment Type']]
    # product = df[["Product", "Price"]].drop_duplicates()
    # order_item = df[["Order Item ID", "Order ID", "Drink ID", "Quantity"]]
    # orders.to_csv("orders_transformed.csv", index=False)
    # product.to_csv("products_transformed.csv", index=False)
    # order_item.to_csv("order_item_transformed.csv", index=False)

    # df.to_csv("extracted_data/transformed_data.csv", index=False)







    """ First - creat uuid , Clean - white spaces, etc , explode m"""