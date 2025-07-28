import psycopg2 as psycopg
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

def get_existing_ids(engine, table_name, id_column):
    query = f"SELECT {id_column} FROM {table_name};"  # Create SQL query to select all values from the ID column
    existing_ids = pd.read_sql(query, engine)  # Read the result into a DataFrame
    # Convert UUIDs to strings
    id_set = set(existing_ids[id_column].astype(str)) 
    return id_set



def load_to_database(product_df, order_df, order_item_df):
    # Load environment variables from .env file
    load_dotenv()  
    # Retrieve database connection details from environment variables
    host_name = os.environ.get("POSTGRES_HOST")
    print("Host from env:", os.getenv("POSTGRES_HOST"))
    database_name = os.environ.get("POSTGRES_DB")
    user_name = os.environ.get("POSTGRES_USER")
    user_password = os.environ.get("POSTGRES_PASSWORD")
    port = os.environ.get("POSTGRES_PORT")

    try:
        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql+psycopg2://{user_name}:{user_password}@{host_name}:{port}/{database_name}")
        existing_product_ids = get_existing_ids(engine, "products", "product_id") # Get the set of existing product IDs already in the database
        product_df["product_id"] = product_df["product_id"].astype(str) # Convert product_id in the DataFrame to string type to match the database
        filtered_product_df = product_df[~product_df["product_id"].isin(existing_product_ids)] # new DataFrame with only the rows whose product_id are not already in the database "~" this negates the isin condition by checking if it does not exist


        if not filtered_product_df.empty: # if not empty adds the products to the db
            filtered_product_df.to_sql("products", engine, if_exists="append", index=False)  # chnaged if_exist from "replace" to "append"
            print(f"Uploaded {len(filtered_product_df)} new products")
        else:
            print("No new products to upload")
        print("Uploaded products table")

        # Upload orders and order items (no filtering, assuming all are new)
        order_df.to_sql("orders", engine, if_exists="append", index=False)
        print("Uploaded orders table")


        order_item_df.to_sql("order_item", engine, if_exists="append", index=False)
        print("Uploaded order_item table")
        return True
        
    except Exception as ex:
        print('Failed to:', ex)
        return False



