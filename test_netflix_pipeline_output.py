import unittest
from datetime import datetime
import pandas as pd
import random
import update_data

class TestNetflixOutput(unittest.TestCase):
    """Test the functions that treat and prepare the data to update the db tables
    """

    @classmethod
    def setUpClass(cls):
        """Load ~1% input samples as a Class attribute
        """

        p = 0.01 # Load approximately 1% of the data
        cls.netflix_titles_sample = pd.read_csv(
            'input_data/netflix_titles.csv',
            header=0,
            # Lambda func will be evaluated against the row indices, returning True if the row should be skipped and False otherwise
            skiprows=lambda i: i>0 and random.random() > p) 
        
    
    def test_create_cast_members_df(self):
        """Test the function update_data.create_cast_members_df()
        Checks:
        * consistency in the number of titles
        * columns names
        * data types
        """

        # Sample of the df that will be updated in the db ()
        cast_members_df = update_data.create_cast_members_df(netflix_df = self.netflix_titles_sample)

        # Test consistency in the number of titles
        titles_qty = self.netflix_titles_sample.show_id.nunique()
        cast_members_df_titles_qty = cast_members_df.show_id.nunique()
        self.assertEqual(cast_members_df_titles_qty,titles_qty,('Amount of titles must remain constant after transformation,'
                                                                ' even if a movie has no record of cast members'))

        # Test cols names
        expected_col_names = ['show_id', 'cast_member']
        actual_col_names = cast_members_df.columns.tolist()
        self.assertEqual(actual_col_names,expected_col_names,'Column names must be the same as expected' )

        # Test data types
        expected_col_dtypes = ['O' for i in range(2)] # all types are object
        actual_col_dtypes =  cast_members_df.dtypes.tolist()
        self.assertEqual(expected_col_dtypes,actual_col_dtypes,'Data types must be the same as expected' )


    def test_threading_gender_request(self):
        """Test the function update_data.update_table_title()
        Checks:
        * API connection
        * API results
        * columns names
        * data types
        """

        # Sample of df that will be used as input in the function
        cast_members_df = update_data.create_cast_members_df(netflix_df = self.netflix_titles_sample)
        # 50 requests should be enough for testing purpose
        cast_members_gender_df =  cast_members_df.sample(50)
        gender_df = update_data.threading_gender_request(cast_members_gender_df,qty_threads = 20)  

        # Test API connection
        qty_request_fail = len(gender_df[gender_df.gender == 'request_failed'])
        # Accepting two occasional failed requests, more than that probably indicates problem
        self.assertLess(qty_request_fail,3,'More than one request failed')
  
        # Test API results
        qty_bad_result = len(gender_df[gender_df.gender == 'UNKNOWN'])
        # 22% of 'UNKNOWN' results are expected. As the sample is small, placing a limit of 50%
        self.assertLess(qty_bad_result,27,r"More than 50% of request with result 'UNKNOWN'")

        # Test cols names
        expected_col_names = ['cast_member', 'gender']
        actual_col_names = gender_df.columns.tolist()
        self.assertEqual(actual_col_names,expected_col_names,'Column names must be the same as expected' )

        # Test data types
        expected_col_dtypes = ['O' for i in range(2)] # all types are object
        actual_col_dtypes =  gender_df.dtypes.tolist()
        self.assertEqual(expected_col_dtypes,actual_col_dtypes,'Data types must be the same as expected' )


    def test_update_table_title(self): 
        """Test the function update_data.update_table_title()
        Checks:
        * columns names
        * data types
        """
        
        # Sample of the df that will be updated in the db
        table_title_df = update_data.update_table_title(
            netflix_df = self.netflix_titles_sample,
            update_db = False)
        
        # Test cols names
        expected_col_names = [
            'show_id', 'type', 'title', 'director',
            'country', 'rating','listed_in', 'description']
        actual_col_names = table_title_df.columns.tolist()
        self.assertEqual(actual_col_names,expected_col_names,'Column names must be the same as expected' )

        # Test data types
        expected_col_dtypes = ['O' for i in range(8)] # all types are object
        actual_col_dtypes =  table_title_df.dtypes.tolist()
        self.assertEqual(expected_col_dtypes,actual_col_dtypes,'Data types must be the same as expected' )


    def test_update_table_movies(self): 
        """Test the function update_data.update_table_movies()
        Checks:
        * columns names
        * data types
        """
        
        # Sample of the df that will be updated in the db
        table_movies_df = update_data.update_table_movies(
            netflix_df = self.netflix_titles_sample,
            update_db = False)
        
        # Test cols names
        expected_col_names = ['show_id', 'date_added', 'release_year', 'movie_length_min']
        actual_col_names = table_movies_df.columns.tolist()
        self.assertEqual(actual_col_names,expected_col_names,'Column names must be the same as expected' )

        # Test data types
        expected_col_dtypes = ['O','datetime64[ns]','int64','int64']
        actual_col_dtypes =  table_movies_df.dtypes.tolist()
        self.assertEqual(expected_col_dtypes,actual_col_dtypes,'Data types must be the same as expected' )


    def test_update_tv_shows(self): 
        """Test the function update_data.update_tv_shows()
        Checks:
        * columns names
        * data types
        """
        
        # Sample of the df that will be updated in the db
        update_table_tv_shows = update_data.update_table_tv_shows(
            netflix_df = self.netflix_titles_sample,
            update_db = False)
        
        # Test cols names
        expected_col_names = ['show_id', 'date_added', 'release_year', 'season_qty']
        actual_col_names = update_table_tv_shows.columns.tolist()
        self.assertEqual(actual_col_names,expected_col_names,'Column names must be the same as expected' )

        # Test data types
        expected_col_dtypes = ['O','datetime64[ns]','int64','int64']
        actual_col_dtypes =  update_table_tv_shows.dtypes.tolist()
        self.assertEqual(expected_col_dtypes,actual_col_dtypes,'Data types must be the same as expected' )


    def save(self):
        """Save a log file
        """

        # Defines the name of the log file (.txt) that will be created
        hoje = datetime.today().strftime(r"%Y-%m-%d %H.%M")
        log_name =  r'log_netflix_pipeline_output_' + str(hoje) + r'.txt'
        log_path = r'log/' + str(log_name)
        with open(log_path , 'w', encoding='utf-8') as file:
            runner = unittest.TextTestRunner(file)
            unittest.main(testRunner = runner, verbosity = 2)


def main():
    STR = TestNetflixOutput()  
    STR.save()


if __name__ == '__main__':
    main()