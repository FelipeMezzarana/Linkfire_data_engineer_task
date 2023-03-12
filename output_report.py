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
                

def create_invalid_data_report():
    """Creates a txt file with invalid_data report
    The report contain:
    Table "title":
        * col "type" -- qty of records != "Movie" or != 'TV Show'
    Table "tv_shows":
        * col "season_qty" -- qty of records <0 or >30
        * col "release_year" -- qty of records <1900 or > current year 
        * col "date_added" -- qty of records <'1997-01-01' or > current date
        * col "release_year"/"date_added" -- qty of records with date_added > release_year
    Table "movies"
        * col "movie_length_min" -- qty of records <0 or >500
        * col "release_year" -- qty of records <1900 or > current year 
        * col "date_added" -- qty of records <'1997-01-01' or > current date
        * col "release_year"/"date_added" -- qty of records with date_added > release_year
    Table cast_members:
        * col "gender" -- qty of records == "UNKNOWN"
        * col "gender" -- % of records == "UNKNOWN"
        * col "gender" -- qty of records == "request_failed"
        * col "gender" -- % of records == "request_failed"
    """  
    
    today = datetime.today().strftime("%Y-%m-%d %H-%M")
    file_name = 'reports/invalid_data_report_' + today + '.txt'
    
    queries_paths = [
        'validate_output_queries/check_invalid_data_table_titles.sql',
        'validate_output_queries/check_invalid_data_table_movies.sql',
        'validate_output_queries/check_invalid_data_table_tv_shows.sql',
        'validate_output_queries/check_invalid_data_table_cast_members.sql']
    table_names = ['titles','movies','tv_shows','cast_members']
    
    # Loop through four queries and tables to write the full report
    counter = 0
    for query_path,table_name in zip(queries_paths,table_names):
        # Read query into DataFrame
        invalid_data_df = query_to_df(query_path)

        with open(file_name, 'a', encoding='utf-8') as f:
            if counter == 0: # Skip a line from the second iteration
                f.write(f'Table {table_name}:\n\n')
            else:
                f.write(f'\nTable {table_name}:\n\n')
            # write results
            for col in invalid_data_df.columns: 
                value = invalid_data_df[col][0]
                if re.search('percent',col): # add % symbol
                    f.write(f'{col} {value:.2f}%\n')
                else:
                    f.write(f'{col} {value}\n')
        counter+=1
                

if __name__ == '__main__':
    create_null_values_report()
    create_invalid_data_report()