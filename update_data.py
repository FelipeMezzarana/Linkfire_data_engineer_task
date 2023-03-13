import pandas as pd
import numpy as np
import math as mt
from datetime import datetime
from time import perf_counter
from sqlalchemy import create_engine
import os
import re
import requests
import queue
import threading 


def create_connection(uid:str = 'root',pwd:str = 'root',host:str = 'localhost',db:str = 'netflix_db'):
    """Returns a connection to a postgresql database
    """

    engige = create_engine(f'postgresql://{uid}:{pwd}@{host}:5432/{db}')
    conn = engige.connect()
    print(f'Connection with {db} created')
    
    return conn


def execute_ddl_statements(sql_files_path:str = 'DDL_queries',**kwargs):
    """Run all SQL scripts in the folder "sql_files_path"
    """

    conn = create_connection(**kwargs)
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


def load_input(check_duplicates:bool = True,file_path:str = 'input_data/netflix_titles.csv',**kwargs):
    """Read a csv file with infos about netflix titles into a DataFrame.  
    * If check_duplicates == True drop any title that already has a record in the database
    """

    netflix_titles_df = pd.read_csv(file_path)    
    
    if check_duplicates == True:
        # finds a list of titles that already have registration
        conn = create_connection(**kwargs)
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
    

def update_table_title(netflix_df:pd.DataFrame, **kwargs):
    """Insert data into "titles" table 
    """    
    
    # Filtering DataFrame 
    df_titles = netflix_df.loc [:,['show_id','type','title',
                                   'director','country','rating',
                                   'listed_in','description']]    
    # Inserting data
    conn = create_connection(**kwargs)
    df_titles.to_sql(
        name = 'titles',
        con = conn,
        if_exists='append',
        index=False,
        method='multi')
    conn.close()
    print('Table titles updated')


def update_table_movies(netflix_df:pd.DataFrame, **kwargs):
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
    conn = create_connection(**kwargs)
    df_movies.to_sql(
        name = 'movies',
        con = conn,
        if_exists='append',
        index=False,
        method='multi'
        )
    
    conn.close()
    print('Table movies updated')


def update_table_tv_shows(netflix_df:pd.DataFrame, **kwargs):
    """Treat and insert data into "tv_shows" table 
    """    
    
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
    conn = create_connection(**kwargs)
    df_tv_shows.to_sql(
        name = 'tv_shows',
        con = conn,
        if_exists='append',
        index=False,
        method='multi'
        )
    
    conn.close()
    print('Table tv_shows updated')


def create_cast_members_df(netflix_df:pd.DataFrame):
    """Returns a DataFrame with a record for each cast member 
    """    

    t_start = perf_counter() # Time counter 
    # Filter cols
    netflix_cast_members_df = netflix_df.loc[:,['show_id','cast']]
    # Convert Cast names to list
    netflix_cast_members_df['cast'] =  [str(name).split(',') for name in netflix_cast_members_df['cast']]
    # Convert show_id to category for performance improvment (approx. 4 times faster)
    netflix_cast_members_df['show_id'] = netflix_cast_members_df['show_id'].astype('category')
    
    # Pivot data: name list -> one name per row
    show_id_list, cast_members_list = [[] for i in range(2)]
    for show_id,i in zip(netflix_cast_members_df.show_id, range(len(netflix_cast_members_df.show_id))):
        cast_members = netflix_cast_members_df[netflix_cast_members_df.show_id == show_id].cast[i]
        for member in cast_members:
            show_id_list.append(show_id)
            cast_members_list.append(member)          
    # Create df
    cast_members_df = pd.DataFrame({'show_id':show_id_list,'cast_member':cast_members_list})
    
    # Treating names and converting 'nan' to NaN 
    cast_members_df.cast_member = cast_members_df.cast_member.apply(
        lambda x:
        x.strip() 
        if x != 'nan' 
        else np.nan)
    
    t_end = perf_counter() # Time counter 
    print(f'Cast Members DataFrame created in: {t_end-t_start:.2f}s')
    
    return cast_members_df


def gender_feature(cast_members_list:list):
    """Returns a DataFrame with the features gender and cast_member 
    * Feature gender will be generated with https://www.aminer.cn/gender/api API
    """   

    name_list,gender_list = [[] for i in range(2)]
    # One request per name
    for name in cast_members_list:
        if name == name: # check for NaN
            request_name = name.replace(' ','+')
            try:
                gender_req = requests.get('https://innovaapi.aminer.cn/tools/v1/predict/gender?name='+  request_name +'&org=')  
                gender = gender_req.json().get('data').get('Final').get('gender')
                name_list.append(name)
                gender_list.append(gender)
            # Request may fail for several reasons
            except:
                name_list.append(name)
                gender_list.append('request_failed')
            
    name_df = pd.DataFrame({'cast_member':name_list,'gender':gender_list})

    return name_df


def threading_gender_request(cast_members_df:pd.DataFrame,qty_threads:int = 100):
    """Run the function gender_feature() with multi threading
    * Runtime reduced from ~116h to 1:18h
    """
    
    # Queue is needed to retrieve data returned with threading
    my_queue = queue.Queue() 
    threads_list = []
    t_start = perf_counter()

    names_list = cast_members_df.cast_member.unique().tolist()
    # len size of each list (for each thread) based on desired thread quantity
    list_len = mt.ceil(len(names_list)/qty_threads) # Round up
    # Splits the name list into several lists to pass each as an thread argument
    list_of_names_list = [names_list[i:i+list_len] for i in range(0,len(names_list),list_len)]
    
    # Create and start threads
    for name_list in list_of_names_list:
        req_thread = threading.Thread(target=lambda q, arg1: q.put(gender_feature(arg1)), args=(my_queue,name_list))
        req_thread.start()   
        threads_list.append(req_thread)
    # Wait for all threads to finish
    for thread in threads_list:
        thread.join() 

    # Retrieve data
    gender_df = pd.DataFrame()
    while not my_queue.empty():
        data = my_queue.get()
        gender_df = pd.concat([gender_df,data]) 
    gender_df.reset_index(drop = True, inplace = True)
    
    t_end = perf_counter() # Time counter 
    print(f'Feature gender created in: {t_end-t_start:.2f}s')
    
    return gender_df


def update_table_cast_members(cast_members_df:pd.DataFrame,gender_df:pd.DataFrame,**kwargs):
    """Merge gender with pivoted cast members table and insert data into "cast_members" table 
    """  

    # Adding gender feature to pivoted cast members df
    cast_members_with_gender_df = pd.merge(cast_members_df,gender_df,how = 'left', on = 'cast_member')  

    # Inserting data
    conn = create_connection(**kwargs)
    cast_members_with_gender_df.to_sql(
        name = 'cast_members',
        con = conn,
        if_exists='append',
        index=False,
        method='multi'
        )
    conn.close()


def etl_pipeline():
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
    # Treat and insert data into "tv_shows" table 
    update_table_tv_shows(new_netflix_titles)
    # Create cast_members_df, with a record for each cast member
    cast_members_df = create_cast_members_df(new_netflix_titles)
    # Create feature gender  
    gender_df = threading_gender_request(cast_members_df,qty_threads = 100)
    # Insert data into "cast_members" table
    update_table_cast_members(cast_members_df,gender_df)


if __name__ == '__main__':
    etl_pipeline()

    


