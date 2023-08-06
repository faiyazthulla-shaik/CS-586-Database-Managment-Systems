import pandas as pd
from tables_code import insert_df_rows_to_table
import re


if __name__ == "__main__":
    df = pd.read_csv("wdidata.csv", header=0)
    insert_df_rows_to_table(df, 'wdi_data')

