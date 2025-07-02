import pandas as pd



#  check data type of each column 
# df = extract_raw_data("raw_data/raw_data_test_transform.csv")
# print(df['Card Number'].map(type).value_counts()) # Everything is a string except for a couple in card number - card number must be strng 



# Adding data to csv 
#data = pd.DataFrame({"Customer Name": "Zara Ibrahim", "Date/Time": "19 Jul 2024 15:56", "Location": "Chessington", "Payment Type":"Card"})

# Updating info in a populated cell 
# df = pd.read_csv("raw_data/raw_data_20_06_2025 testing.csv")
# df.loc[(df["Customer Name"] == "Chinedu Okafor") & (df["Branch"] == "Epsom"), "Drinks Ordered"] = df.loc[(df["Customer Name"] == "Chinedu Okafor") & (df["Branch"] == "Epsom"), "Drinks Ordered"] + ", Latte - £3.5"  # if name = chinedu okafor, update the drinks ordered coloumn and add a latte - £3.5
# df.to_csv("raw_data/raw_data_20_06_2025 testing.csv", index=False)