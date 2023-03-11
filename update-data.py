from sqlalchemy import create_engine
import os
import re


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


if __name__ == '__main__':
    execute_ddl_statements()

    


