import pandas as pd


"""Extract:
Imporing raw data csv file - storing data into a dataframe using pandas """
def extract_raw_data(csv):
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