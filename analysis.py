import os
from dotenv import load_dotenv
import psycopg2 as psycopg

# Load environment variables from .env file
load_dotenv()
host_name = os.environ.get("POSTGRES_HOST")
print("Host from env:", os.getenv("POSTGRES_HOST"))
database_name = os.environ.get("POSTGRES_DB")
user_name = os.environ.get("POSTGRES_USER")
user_password = os.environ.get("POSTGRES_PASSWORD")

def print_query(cursor, query, params=None, label=None, prefix=""):
    if label:
        print(label)
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)  # only run with parameters if they exist

    for row in cursor.fetchall():
        if len(row) == 3:      
            print(f"{row[0]} - {prefix}{row[1]} - {row[2]}%")
        if len(row) == 2:      
            print(f"{row[0]} - {prefix}{row[1]}")   # blank line for spacing between query outputs

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
        with connection.cursor() as cursor:



            """SQL QUERIES"""


            """ Top Product (Cash) """
            sql_query1 = """ WITH revenue AS (SELECT p.product, SUM(p.price * oi.quantity) AS revenue
                                FROM order_item AS oi JOIN products AS p ON oi.product_id = p.product_id
                                GROUP BY p.product),

                                total_revenue AS (SELECT SUM(revenue) AS total_rev FROM revenue)

                            SELECT r.product, 
                            ROUND(CAST(r.revenue AS NUMERIC),0) AS revenue, 
                            ROUND(CAST(r.revenue AS NUMERIC) * 100 / CAST(tr.total_rev AS NUMERIC), 0) AS percentage
                             FROM revenue r, total_revenue tr
                                ORDER BY revenue"""

            print_query(cursor, sql_query1, label="Top Product (Revenue)", prefix="£")



            """\nTop Product (Units)\n"""
            sql_query2 = "SELECT p.product, SUM(oi.quantity) AS quantity FROM order_item AS oi\
                    INNER JOIN products AS p ON oi.product_id = p.product_id\
                    GROUP BY p.product\
                    ORDER BY quantity DESC"
            
            print_query(cursor, sql_query2, label="Top Product (Units)")

            """ Top Store (Cash) """
            sql_query3 = "SELECT branch, ROUND(CAST(SUM(oi.quantity * p.price)AS NUMERIC),0) AS quantity FROM order_item as oi\
                        JOIN orders AS o ON oi.order_id = o.order_id JOIN products AS p ON oi.product_id = p.product_id \
                        GROUP BY branch\
                        ORDER BY quantity DESC"
            print_query(cursor, sql_query3, label="Top Store (Revenue)", prefix="£")
                    

            """ Top Store (Units) """
            sql_query4 = "SELECT branch, SUM(oi.quantity) AS quantity FROM order_item as oi\
                        JOIN orders AS o ON oi.order_id = o.order_id\
                        GROUP BY branch\
                        ORDER BY quantity DESC"
            print_query(cursor, sql_query4, label="Top Store (Units)")


            """ Top Products by store (Cash & Units)  """

            branches = ["Richmond", "Soho", "Brixton"]
            
        
            sql_query5 = "SELECT p.product AS product, ROUND(CAST(SUM(oi.quantity * p.price)AS NUMERIC),0) AS revenue FROM order_item as oi\
                        JOIN orders AS o ON oi.order_id = o.order_id JOIN products AS p ON oi.product_id = p.product_id\
                        WHERE branch = %s\
                        GROUP BY product\
                        ORDER BY revenue DESC"
            for branch in branches:
                label = f"Top Products by store (Revenue) - {branch.upper()}"
                print_query(cursor, sql_query5, params=(branch,), label=label, prefix="£")


            sql_query6 = "SELECT p.product AS product, SUM(oi.quantity) AS quantity FROM order_item as oi\
                        JOIN orders AS o ON oi.order_id = o.order_id JOIN products AS p ON oi.product_id = p.product_id\
                        WHERE branch = %s\
                        GROUP BY product\
                        ORDER BY quantity DESC"
            for branch in branches:
                label = f"Top Products by store (Units) - {branch.upper()}"
                print_query(cursor, sql_query6, params=(branch,), label=label)


            """ Revenue by Day """  
            sql_query7 = "\
            SELECT DATE(date_time) AS date, ROUND(CAST(SUM(oi.quantity * p.price) AS NUMERIC),0) AS revenue from orders AS o JOIN order_item as oi ON o.order_id = oi.order_id JOIN products AS p ON oi.product_id = p.product_id \
                GROUP BY date\
                ORDER BY date"
            print_query(cursor, sql_query7, label="Revenue by Day", prefix="£")


            """ Revenue by Hour """  
            sql_query8 = " SELECT TO_CHAR(date_time, 'HH24:00') AS time, ROUND(CAST(SUM(quantity * price) AS NUMERIC),0) AS revenue FROM orders\
                INNER JOIN order_item\
                ON orders.order_id = order_item.order_id\
                INNER JOIN products\
                ON order_item.product_id = products.product_id\
                GROUP BY time\
                ORDER BY time"
            print_query(cursor, sql_query8, label="Revenue by Hour", prefix="£")


except Exception as ex:
    print('Failed to:', ex)
