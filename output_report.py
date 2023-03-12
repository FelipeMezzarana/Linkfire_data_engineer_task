from update_data import create_connection
import pandas as pd
from datetime import datetime
import re

def query_to_df(query_path:str):
    """Returns a DataFrame from the query result
    """

    conn = create_connection()
    with open(query_path, 'r') as sql_file:
            query = sql_file.read()
            df = pd.read_sql_query(query, conn)  
    conn.close()
    
    return df


def create_null_values_report():
    """Creates a txt file with null values report
    The report contain:
    * Null values quantity for each columns in each table
    * Null values percentage for each columns in each table
    """  
    
    today = datetime.today().strftime("%Y-%m-%d %H-%M")
    file_name = 'reports/null_values_report_' + today + '.txt'
    
    queries_paths = [
        'validate_output_queries/check_null_table_titles.sql',
        'validate_output_queries/check_null_table_movies.sql',
        'validate_output_queries/check_null_table_tv_shows.sql',
        'validate_output_queries/check_null_table_cast_members.sql']
    table_names = ['titles','movies','tv_shows','cast_members']
 
    # Loop through four queries and tables to write the full report
    counter = 0
    for query_path,table_name in zip(queries_paths,table_names):
        # Read query into DataFrame
        null_titles_df = query_to_df(query_path)
        # Identifies columns with null values
        cols_with_null_values = []
        for col in null_titles_df.columns:
            if null_titles_df[col][0] != 0:
                cols_with_null_values.append(col)
        # Creates report in txt only with columns in which there are NULL values
        with open(file_name, 'a', encoding='utf-8') as f:
            if counter == 0: # Skip a line from the second iteration
                f.write(f'Table {table_name}:\n\n')
            else:
                f.write(f'\nTable {table_name}:\n\n')
            # check for cols with null
            if len(cols_with_null_values) != 0:
                # write results
                for col in cols_with_null_values: 
                    value = null_titles_df[col][0]
                    if re.search('percent',col): # add % symbol
                        f.write(f'{col} {value:.2f}%\n')
                    else:
                        f.write(f'{col} {value}\n')
            else:
                f.write(f'None columns with null values\n')
        counter+=1
                

if __name__ == '__main__':
    create_null_values_report()