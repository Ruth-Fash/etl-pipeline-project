import psycopg2 as psycopg
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


def load_to_database(product_df, order_item_df, order_df):
    # Load environment variables from .env file
    load_dotenv()
    host_name = os.environ.get("POSTGRES_HOST")
    print("Host from env:", os.getenv("POSTGRES_HOST"))
    database_name = os.environ.get("POSTGRES_DB")
    user_name = os.environ.get("POSTGRES_USER")
    user_password = os.environ.get("POSTGRES_PASSWORD")
    port = os.environ.get("POSTGRES_PORT")

    try:
        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql+psycopg2://{user_name}:{user_password}@{host_name}:{port}/{database_name}")
        product_df.to_sql("products", engine, if_exists="append", index=False)  # chnaged if_exist from "replace" to "append"
        print("Uploaded products table")
        order_item_df.to_sql("order_item", engine, if_exists="append", index=False)
        print("Uploaded order_item table")
        order_df.to_sql("orders", engine, if_exists="append", index=False)
        print("Uploaded orders table")
        
    except Exception as ex:
        print('Failed to:', ex)

