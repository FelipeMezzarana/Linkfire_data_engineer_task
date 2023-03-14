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
        'sql_queries/validate_output_queries/check_null_table_titles.sql',
        'sql_queries/validate_output_queries/check_null_table_movies.sql',
        'sql_queries/validate_output_queries/check_null_table_tv_shows.sql',
        'sql_queries/validate_output_queries/check_null_table_cast_members.sql']
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
        'sql_queries/validate_output_queries/check_invalid_data_table_titles.sql',
        'sql_queries/validate_output_queries/check_invalid_data_table_movies.sql',
        'sql_queries/validate_output_queries/check_invalid_data_table_tv_shows.sql',
        'sql_queries/validate_output_queries/check_invalid_data_table_cast_members.sql']
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
                

def create_analytical_report():
    """Creates a txt file "analytical_report" with the answers to all the questions in step 5
    """     

    today = datetime.today().strftime("%Y-%m-%d %H-%M")
    file_name = 'reports/analytical_report_' + today + '.txt'
    
    # 1º Query
    query_1_path = 'sql_queries/analysis_queries/01_most_common_first_name.sql'
    most_common_first_name_df = query_to_df(query_1_path)
    # Write results
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(f'01 - What is the most common first name among actors and actresses?\n')
        f.write(f'{most_common_first_name_df.first_name[0]} - {most_common_first_name_df.first_name_qty[0]} Records')
        
    # 2º Query   
    query_2_path = 'sql_queries/analysis_queries/02_movie_with_longest_timespan.sql'
    movie_with_longest_timespan = query_to_df(query_2_path)
    # Checking duplicate values in first position (draw)
    draw_qty = movie_with_longest_timespan.loc[
        movie_with_longest_timespan.year_timespan == max(movie_with_longest_timespan.year_timespan),[
        'date_added', 'release_year', 'year_added', # Loc cols related to date
        'month_added', 'day_added', 'year_timespan']].duplicated().sum() + 1 # No draw = 0 duplicates
    # Write results
    with open(file_name, 'a', encoding='utf-8') as f:
        f.write(f'\n\n02 - Which Movie had the longest timespan from release to appearing on Netflix?\n')
        for i in range(draw_qty): # loop through draw values
            f.write(f'{movie_with_longest_timespan.title[i]} - '
                    f'release_year {movie_with_longest_timespan.release_year[i]} '
                    f'date_added {movie_with_longest_timespan.date_added[i]}\n')
    
    # 3º Query
    query_3_path = 'sql_queries/analysis_queries/03_month_most_new_releases.sql'
    month_most_new_releases = query_to_df(query_3_path)
    # Write results
    with open(file_name, 'a', encoding='utf-8') as f:
        f.write(f'\n03 - Which Month of the year had the most new releases historically?\n')
        f.write(f'Month {month_most_new_releases.month_added[0]:.0f} - '
                f'{month_most_new_releases.qty_titles_added[0]} Titles added')
    
    # 4º Query
    query_4_path = 'sql_queries/analysis_queries/04_tv_shows_largest_increase_year_on_year.sql'
    tv_shows_largest_increase = query_to_df(query_4_path)
    # Write results
    with open(file_name, 'a', encoding='utf-8') as f:
        f.write(f'\n\n04 - Which year had the largest increase year on year (percentage wise) for TV Shows?\n')
        f.write(f'{tv_shows_largest_increase.year_added[0]:.0f} - '
                f'{tv_shows_largest_increase.increase_percent[0]:.2f}% Increase percent.')

    # 5º Query
    query_5_path = 'sql_queries/analysis_queries/05_actresses_more_than_one_movie_with_wood.sql'
    actresses_with_woody = query_to_df(query_5_path)
    # Write results
    with open(file_name, 'a', encoding='utf-8') as f:
        f.write(f'\n\n05 - List the actresses that have appeared in a movie with Woody Harrelson more than once?\n')
        for i in range(len(actresses_with_woody)):
            f.write(f'{actresses_with_woody.cast_member[i]} - '
                    f'{actresses_with_woody.qty_movies_with_woody[i]:.0f} Movies with Woody Harrelson\n')


def reports_pipeline():
    """Generates three reports on folder Linkfire_data_engineer_task/reports
    """ 
    
    create_null_values_report()
    create_invalid_data_report()
    create_analytical_report()


if __name__ == '__main__':
    reports_pipeline()