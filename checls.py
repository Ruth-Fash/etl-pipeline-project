import pandas as pd
from extract import extract_raw_data


#  check data type of each column 
df = extract_raw_data("raw_data/raw_data_test_transform.csv")
print(df['Card Number'].map(type).value_counts())
# Everything is a string except for a couple in card number - card number must be strng 