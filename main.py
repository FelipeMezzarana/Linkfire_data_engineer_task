import subprocess
import update_data
import output_report

if __name__ == '__main__':

    # Run the input testes from CLI
    subprocess.run(
        ["python3", "test_netflix_pipeline_input.py"],
        check = True) # Raise error if process exits with a non-zero exit code (stop the pipeline)
    # Run the output testes from CLI
    subprocess.run(
        ["python3", "test_netflix_pipeline_output.py"],
        check = True)
    # Creates tables(if necessary) and etl data (without duplicate data)
    update_data.etl_pipeline()
    # Generates 3 reports on updated data in the db: null values, invalid data and analytical
    output_report.reports_pipeline()



   


