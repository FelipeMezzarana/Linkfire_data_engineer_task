import subprocess


if __name__ == '__main__':

    # Run the input testes from CLI
    subprocess.run(
        ["python3", "test_netflix_pipeline_input.py"],
        check = True # Raise error if process exits with a non-zero exit code (stop the pipeline)
        )
    
   


