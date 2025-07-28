import csv
import random
from datetime import datetime, timedelta
import os

# Customer names from the original data
customers = [
    'Maria Lopez', 'Fatima Yusuf', 'James Carter', 'Chinedu Okafor', 'Sophie Dubois',
    'Leila Ahmed', 'Anna Smith', 'Grace Kim', 'Carlos Reyes', 'Nina Patel',
    'Oliver Twist', 'Aisha Bello', 'Jake Thompson', 'Dave Johnson', 'Chen Wei'
]

# Drink options with prices (matching the original data patterns)
drinks = {
    'Espresso': ['£2.0', '2.0'],
    'Americano': ['£2.2', '2.2', ' 2.2 '],
    'Latte': ['£2.5', '2.5', ' 2.5 '],
    'Cappuccino': ['£3.0', '3.0', ' 3.0 '],
    'Flat White': ['£2.75', '2.75', ' 2.75 '],
    'Macchiato': ['£3.1', '3.1', ' 3.1 '],
    'Mocha': ['£3.75', '3.75', ' 3.75 ']
}

# Branch names
branches = ['Brixton', 'Richmond', 'Soho']

# Payment types
payment_types = ['Card', 'Cash']

# Date formats (matching inconsistency in original data)
date_formats = [
    '%d-%m-%Y %H:%M',
    '%Y/%m/%d %H:%M',
    '%d %b %Y %H:%M',
    '%d/%m/%Y',
    '%Y/%m/%d %H:%M',
    '%dth %B %Y'
]

def generate_card_number():
    """Generate realistic card numbers (some with scientific notation like in original)"""
    if random.random() < 0.3:  # 30% chance of scientific notation
        return f"{random.uniform(1, 9.9):.15e}+{random.randint(15, 17)}"
    else:
        return str(random.randint(10**14, 10**17))

def generate_order():
    """Generate a realistic drink order"""
    num_drinks = random.choices([1, 2, 3, 4], weights=[40, 35, 20, 5])[0]
    order_items = []
    
    for _ in range(num_drinks):
        drink = random.choice(list(drinks.keys()))
        price = random.choice(drinks[drink])
        order_items.append(f"{drink} - {price}")
    
    return ', '.join(order_items)

def format_date(date, time_hour=None):
    """Format date in various formats matching the original inconsistency"""
    if time_hour is None:
        time_hour = random.randint(7, 22)
    
    time_minute = random.randint(0, 59)
    datetime_obj = date.replace(hour=time_hour, minute=time_minute)
    
    format_choice = random.choice(date_formats)
    
    if format_choice == '%dth %B %Y':
        return f"{date.day}th {date.strftime('%B %Y')}"
    else:
        return datetime_obj.strftime(format_choice)

def generate_daily_sales(branch, date, num_transactions=None):
    """Generate sales data for a specific branch and date"""
    if num_transactions is None:
        # Different base volumes for different branches
        if branch == 'Soho':  # Busiest location
            base_transactions = random.randint(90, 120)
        elif branch == 'Brixton':  # Medium volume
            base_transactions = random.randint(70, 100)
        else:  # Richmond - quieter location
            base_transactions = random.randint(60, 85)
        
        # Weekend boost (20-40% more customers)
        if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            weekend_multiplier = random.uniform(1.2, 1.4)
            base_transactions = int(base_transactions * weekend_multiplier)
        
        # Weekday variations (Monday/Tuesday slightly quieter)
        if date.weekday() in [0, 1]:  # Monday, Tuesday
            base_transactions = int(base_transactions * random.uniform(0.85, 0.95))
        
        num_transactions = base_transactions
    
    sales_data = []
    
    for _ in range(num_transactions):
        # Some entries might have empty customer names (like in original)
        if random.random() < 0.05:  # 5% chance of empty name
            customer = ''
        else:
            customer = random.choice(customers)
        
        formatted_date = format_date(date)
        payment_type = random.choice(payment_types)
        
        # Some entries might have empty payment type
        if random.random() < 0.02:  # 2% chance
            payment_type = ''
        
        order = generate_order()
        
        # Some orders might be "Invalid Order String"
        if random.random() < 0.01:  # 1% chance
            order = 'Invalid Order String'
        
        # Card number (only for card payments, sometimes empty)
        card_number = ''
        if payment_type == 'Card':
            if random.random() < 0.9:  # 90% of card payments have card numbers
                card_number = generate_card_number()
        
        sales_data.append({
            'Customer Name': customer,
            'Date/Time': formatted_date,
            'Branch': branch,
            'Payment Type': payment_type,
            'Drinks Ordered': order,
            'Card Number': card_number
        })
    
    return sales_data

def create_csv_files():
    """Create CSV files for each branch and date"""
    
    # Create directory for output files
    if not os.path.exists('sales_data'):
        os.makedirs('sales_data')
    
    # Starting date: April 25, 2024 (day after your example date)
    start_date = datetime(2024, 4, 25)
    
    for branch in branches:
        for day_offset in range(7):  # 7 days
            current_date = start_date + timedelta(days=day_offset)
            
            # Generate sales data for this branch and date
            daily_sales = generate_daily_sales(branch, current_date)
            
            # Create filename
            filename = f"{branch.lower()}_sales_{current_date.strftime('%Y%m%d')}.csv"
            filepath = os.path.join('sales_data', filename)
            
            # Write to CSV
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Customer Name', 'Date/Time', 'Branch', 'Payment Type', 'Drinks Ordered', 'Card Number']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(daily_sales)
            
            print(f"Created {filename} with {len(daily_sales)} transactions")

# Generate the data
if __name__ == "__main__":
    create_csv_files()
    print(f"\nGenerated 21 CSV files (7 days × 3 branches)")
    print("Files saved in 'sales_data' directory")
    print("\nDate range: April 25, 2024 to May 1, 2024")
    print("Branches: Brixton, Richmond, Soho")