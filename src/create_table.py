from __init__ import initialize
import pandas as pd
import psycopg2
from psycopg2 import sql


if __name__ == '__main__':
    initialize()

db_params = {
    'dbname': 'stocks',
    'user': 'postgres',
    'password': 'siravitxau',
    'host': 'localhost',
    'port': '5432'
}

# Load CSV 'stock_prices.csv' -> Pandas DataFrame
csv_file = r"input\stock_prices.csv" # path
df = pd.read_csv(csv_file, header = [0, 1]) # the first 2 rows is a header (industries and stock name)

# Rename column name from fisrt column -> date , second column and so on will contain stock name with industry in this format (INDUS_NAME) for further adjust
columns = ['date'] + [
    f"{industry}_{stock}" for industry, stock in zip(df.columns.get_level_values(0)[1:], df.columns.get_level_values(1)[1:])
]
df.columns = columns

# Drop 1st Nan Stock Price row and cleansing for NaN data
df = df.iloc[1:].reset_index(drop = True)
df = df.where(pd.notnull(df), None)

print(df.head())

create_table_query = \
    f"""
    CREATE TABLE IF NOT EXISTS stock_prices (
        date DATE PRIMARY KEY,
        {', '.join([f'"{col}" FLOAT' for col in columns[1:]])})
    """

# Connect to PostgreSQL stocks database
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # Create Table
    cursor.execute(create_table_query)
    conn.commit()
    
    # Insert data to table
    for index, row in df.iterrows():
        
        insert_query = sql.SQL(
    """ INSERT INTO stock_prices ({fields})
        VALUES ({values})
        ON CONFLICT (date) DO NOTHING
    """
        ).format(
            fields = sql.SQL(', ').join(map(sql.Identifier, columns)),
            values = sql.SQL(', ').join(map(sql.Literal, row)))
        cursor.execute(insert_query)
    
    conn.commit()
    print("Data ingested Successfully into the database.")

except Exception as e:
    print(f"An error occurred: {e}")

finally: # Close database connection
    if conn:
        cursor.close()
        conn.close()
    
    