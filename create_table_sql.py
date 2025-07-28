import sys
sys.path.append('/Users/ruthfashogbon/Desktop/Generation/ruths_mini_project/week6') 

import psycopg2 as psycopg
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
host_name = os.environ.get("POSTGRES_HOST")
print("Host from env:", os.getenv("POSTGRES_HOST"))
database_name = os.environ.get("POSTGRES_DB")
user_name = os.environ.get("POSTGRES_USER")
user_password = os.environ.get("POSTGRES_PASSWORD")

try:
    print("Opening connection....")

    # Establishing a connection 
    with psycopg.connect(f"""
        host={host_name}
        dbname={database_name}
        user={user_name}
        password={user_password}
        """) as connection:

        print('Opening cursor...')
        cursor = connection.cursor()

        print('Creating new table...')

        #sql = "CREATE TABLE products (product_id UUID PRIMARY KEY, product TEXT NOT NULL, price FLOAT NOT NULL)"
        sql = "CREATE TABLE order_item (order_item_id UUID PRIMARY KEY, order_id UUID NOT NULL REFERENCES orders(order_id), product_id UUID NOT NULL REFERENCES products(product_id), quantity INTEGER NOT NULL)"
        cursor.execute(sql)

        # cursor.description contains metadata about the columns in the result set
        # desc[0] holds the name of each column, so we extract just the names into a list
        cursor.execute("SELECT * FROM order_item LIMIT 0") 
        column_names = [desc[0] for desc in cursor.description] 
        print("Column names:", column_names)

        print('Committing...')
        connection.commit()

except Exception as ex:
    print('Failed to:', ex)

