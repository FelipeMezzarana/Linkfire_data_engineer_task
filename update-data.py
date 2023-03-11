from sqlalchemy import create_engine
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

def create_connection(uid:str = 'root',pwd:str = 'root',host:str = 'localhost',db:str = 'netflix_db'):
    """Returns a connection to a postgresql database
    """

    engige = create_engine(f'postgresql://{uid}:{pwd}@{host}:5432/{db}')
    conn = engige.connect()
    print(f'Connection with {db} created')
    
    return conn


def execute_ddl_statements(sql_files_path:str = 'DDL_querys',*kargs):
    """Run all SQL scripts in the folder "sql_files_path"
    """

    conn = create_connection(*kargs)
    # checks for .sql files
    folder_files = sorted(os.listdir(sql_files_path)) # Sorting ensures alphabetical order of execution
    for file in folder_files:
        if re.search('.sql',file):
            # read and run query
            with open(sql_files_path + r'/'+ file, 'r') as sql_file:
                query = sql_file.read()
                conn.execute(query)
                print(f'Query executed:\n{query}')

    conn.close()  


def load_input(check_duplicates:bool = True,file_path:str = 'input_data/netflix_titles.csv'):
    """Read a csv file with infos about netflix titles into a DataFrame.  
    * If check_duplicates == True drop any title that already has a record in the database
    """

    netflix_titles_df = pd.read_csv(file_path)    
    
    if check_duplicates == True:
        # finds a list of titles that already have registration
        conn = create_connection()
        check_records_query = (
            'SELECT '
            '   show_id '
            'FROM '
            '   titles')
        titles_with_record_df = pd.read_sql_query(check_records_query,conn)
        conn.close()
        titles_with_record = titles_with_record_df.show_id.tolist()
        # Filters titles with records, avoiding inserting duplicate data in the db
        netflix_filtered_titles_df = netflix_titles_df[~netflix_titles_df.show_id.isin(titles_with_record)]
        print('Input loaded (checking for duplicate records)')
        return netflix_filtered_titles_df
    else:
        print('Input loaded (WITHOUT checking for duplicate records)')
        return netflix_titles_df
    

def update_table_title(netflix_df:pd.DataFrame, *kargs):
    """Insert data into "titles" table 
    """    
    
    # Filtering DataFrame 
    df_titles = netflix_df.loc [:,['show_id','type','title',
                                   'director','country','rating',
                                   'listed_in','description']]    
    # Inserting data
    conn = create_connection()
    df_titles.to_sql(
        name = 'titles',
        con = conn,
        if_exists='append',
        index=False, *kargs)
    conn.close()
    print('Table titles updated')


def update_table_movies(netflix_df:pd.DataFrame, *kargs):
    """Treat and insert data into "movies" table 
    """    
    
    # Filtering DataFrame 
    df_movies = netflix_df.loc[
        netflix_df.type == 'Movie',
        ['show_id','date_added','release_year','duration']]
    
    # Converting date to timestamp
    df_movies['date_added'] = df_movies['date_added'].apply(
        lambda x: 
        datetime.strptime(x.strip(), '%B %d, %Y')   
        if x == x  # Check for NaN values
        else np.nan) 

    # Convert duration to movie length in minutes (int)
    df_movies['duration'] = df_movies['duration'].apply(
        lambda x: 
        int(re.findall('\d+',x)[0])
        if re.search('\d+',str(x)) 
        else np.nan) # If no numbers are identified fill in with NaN
    df_movies.rename(columns = {'duration':'movie_length_min'},inplace = True)

    # Inserting data
    conn = create_connection()
    df_movies.to_sql(
        name = 'movies',
        con = conn,
        if_exists='append',
        index=False,
        *kargs)
    
    conn.close()
    print('Table movies updated')


def update_table_tv_shows(netflix_df:pd.DataFrame, *kargs):
    """Treat and insert data into "tv_shows" table 
    """    
    
    global df_tv_shows
    # Filtering DataFrame 
    df_tv_shows = netflix_df.loc[
        netflix_df.type == 'TV Show',
        ['show_id','date_added','release_year','duration']]
    
    # Converting date to timestamp
    df_tv_shows['date_added'] = df_tv_shows['date_added'].apply(
        lambda x: 
        datetime.strptime(x.strip(), '%B %d, %Y')   
        if x == x  # Check for NaN values
        else np.nan)    
    
    # Convert duration to number of seasons (int)
    df_tv_shows['duration'] = df_tv_shows['duration'].apply(
        lambda x: 
        int(re.findall('\d+',x)[0])
        if re.search('\d+',x) 
        else np.nan) # If no numbers are identified fill in with NaN
    df_tv_shows.rename(columns = {'duration':'season_qty'},inplace = True)

    # Inserting data
    conn = create_connection()
    df_tv_shows.to_sql(
        name = 'tv_shows',
        con = conn,
        if_exists='append',
        index=False,
        *kargs)
    
    conn.close()
    print('Table tv_shows updated')


def main():
    """Create tables, treat and insert data
    """   

    # Create tables
    execute_ddl_statements()
    # Load raw df (without duplicates)
    new_netflix_titles = load_input()
    # Insert data into "titles" table
    update_table_title(new_netflix_titles)
    # Treat and insert data into "movies" table 
    update_table_movies(new_netflix_titles)
    # Treat and insert data into "movies" table 
    update_table_tv_shows(new_netflix_titles)


if __name__ == '__main__':
    main()

    


