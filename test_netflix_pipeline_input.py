import unittest
from datetime import datetime
import pandas as pd
from update_data import create_connection
import os


class TestNetflixInput(unittest.TestCase):
    """Runs data quality and connection tests
    """

    @classmethod
    def setUpClass(cls):
        """Load input as a Class attribute
        """

        # Checks if input exists and load it only once for all tests
        if 'netflix_titles.csv' in os.listdir('input_data/'):
            cls.netflix_titles_df = pd.read_csv('input_data/netflix_titles.csv') 
        else:
            raise Exception('Input file not found, input must be: input_data/netflix_titles.csv')


    def test_check_connection(self): 
        """Check if database connection is available
        """

        try:
            conn = create_connection()
            conn_available = True
            conn.close()
        except:
            conn_available = False

        self.assertTrue(conn_available, ('Attempt to connect to database failed'
                                         '\nDatabase must be initialized with Docker using the file:'
                                         '\nLinkfire_data_engineer_task/database/volume/docker-compose.yaml'))


    def check_cols_names(self):
        """Input column names must always be the same
        * Otherwise errors will occur during the transformation phase
        """

        expected_names = [
            'show_id','type','title','director','cast','country','date_added',
            'release_year','rating','duration','listed_in','description']
        actual_col_names = self.netflix_titles_df.columns.tolist()

        self.assertEqual(actual_col_names,expected_names)


    def test_missing_values(self):
        """Tests for missing values in columns where this is not allowed
        """

        # Test show_id
        missing_show_id = self.netflix_titles_df.show_id.isna().sum()
        self.assertEqual(missing_show_id,0,'There can be no records without show_id field')
        # Test type
        missing_type = self.netflix_titles_df.type.isna().sum()
        self.assertEqual(missing_type,0,'There can be no records without type field')


    def test_duplicate(self):
        """Check for duplicate show_id
        """
        duplicate_show_id_qtd = self.netflix_titles_df.show_id.duplicated().sum()
        self.assertEqual(duplicate_show_id_qtd,0,'There can be no duplicate records of show_id field')


    def test_date_format(self):
        """date is expected to be in the format: '%B %d, %Y' ex: 'March 31, 2017'
        * leading and trailing whitespace character are acceptable
        """

        date_with_error = None
        show_id_with_error = None
        date_format_ok = True
        for date,show_id in zip(self.netflix_titles_df.date_added,self.netflix_titles_df.show_id):
            if date == date: # Skip NaN
                # Try to convert
                try:
                    datetime.strptime(date.strip(), "%B %d, %Y")
                # If fails catch problematic value and respective show_id, facilitating debug
                except:
                    date_format_ok = False
                    date_with_error = date
                    show_id_with_error = show_id
                    break

        self.assertTrue(date_format_ok, ('Date with unexpected formatting found.'
                                         f'\nshow_id:{show_id_with_error}\ndate with error:{date_with_error}'))


    def test_titles_types(self):
        """Title types must be "TV Show" or "Movie"
        * The data model predicts the division of tables between these two categories,
          therefore data that does not fit into one of these options may be lost
        """

        unique_titles_types = self.netflix_titles_df.type.unique()
        # np.all() requires that all values are true
        self.assertIn(unique_titles_types.all(), ['TV Show','Movie'])


    def save(self):
        """Save a log file
        """

        # Defines the name of the log file (.txt) that will be created
        hoje = datetime.today().strftime(r"%Y-%m-%d %H.%M")
        log_name =  r'log_netflix_pipeline_input_' + str(hoje) + r'.txt'
        log_path = r'log/' + str(log_name)
        with open(log_path , 'w', encoding='utf-8') as file:
            runner = unittest.TextTestRunner(file)
            unittest.main(testRunner = runner, verbosity = 2)


def main():
    STR = TestNetflixInput()  
    STR.save()


if __name__ == '__main__':
    main()