import pandas as pd
import psycopg2

def query_sql_to_df():
    db_params = {
        'host' : 'localhost',
        'database' : 'stocks',
        'user' : 'postgres',
        'password' : 'siravitxau',
        'port' : 5432
    }
    query = "SELECT * FROM stock_prices;"
    
    try:
        conn = psycopg2.connect(**db_params)
        
        df = pd.read_sql_query(query, conn)
        
        df["date"] = pd.to_datetime(df["date"])
        
        # df.to_csv("input\output_query.csv", index = False) [Optional]
        return df
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        
    finally:
        if conn:
            conn.close()
    
if __name__ == '__main__':
    df = query_sql_to_df()

