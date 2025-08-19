import os
from dotenv import load_dotenv
import psycopg2 as psycopg
import pandas as pd

# Load environment variables from .env file
load_dotenv()
host_name = os.environ.get("POSTGRES_HOST")
print("Host from env:", os.getenv("POSTGRES_HOST"))
database_name = os.environ.get("POSTGRES_DB")
user_name = os.environ.get("POSTGRES_USER")
user_password = os.environ.get("POSTGRES_PASSWORD")


print("Opening connection....")
# Establishing a connection 
with psycopg.connect(f"""
    host={host_name}
    dbname={database_name}
    user={user_name}
    password={user_password}
    """) as connection:


        df = pd.read_sql("SELECT * FROM products", connection)  # reading form the database into a df 
        df.to_csv("valid_drinks_list.csv", index=False) # saving to df to a csv

connection.close()