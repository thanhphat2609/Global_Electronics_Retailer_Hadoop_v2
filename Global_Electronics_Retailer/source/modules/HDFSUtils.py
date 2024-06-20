import os
import re
import subprocess

class HDFSUtils:

    def __init__(self) -> None:
        pass

    # Function check batch_run
    def check_batch_run(self, project_name, executionDate):
        """
            Check batch_run of the day

            - Args:
                executionDate: Execution Date

            - Return
                Code of the command line
        """

        # Initial value
        batch_run = 1
        hdfs_paths = None
        base_path = None
        try:
            # Base path on HDFS
            base_path = f'hdfs://localhost:9000/lakehouse/LH_{project_name}/Files/log/{executionDate}/'

            # Get all paths in HDFS
            hdfs_paths = os.popen(f"hadoop fs -ls {base_path}").read()

            # Process each line
            numberofline = 0
            for line in hdfs_paths.split('\n'):
                line = line.strip()  # Remove leading/trailing whitespace
                if line and not line.startswith("Found"):  # Skip empty lines and lines starting with "Found"
                    # parts = line.split()
                    numberofline = numberofline + 1

            batch_run = batch_run + numberofline
                

        except Exception as e:
            print("Error:", e)
            batch_run = None

        # print("Batch run: ", batch_run)
        return batch_run

    # Function run_cmd code
    def run_cmd(self, args_list):
        """
            Function run cmd

            - Args:
                args_list for multiple test of cmd

            - Return
                Code of the command line
        """
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        proc.communicate()

        return proc.returncode
    
    # Check exist path
    def check_exist_data(self, executionDate, project, tblName):
        """
            Function check_exist_data

            - Args:
                executionDate: Execution Date.
                project: Name of the project.
                tablename: Name for table of path.

            - Return
                Code 0: Exist file, 1: Not exist file
        """
        executionDate = executionDate.split("-")

        # Partition data by Arguments
        year = executionDate[0]
        month = executionDate[1]
        day = executionDate[2]

        path_check = f"/lakehouse/LH_{project}/Files/Bronze/{tblName}/year={year}/month={month}/day={day}/"
        cmd = ['hdfs', 'dfs', '-test', '-d', path_check]

        code = self.run_cmd(cmd)

        return code


    def get_new_version(self, executionDate, project, tablename):
        """
        Function Get new version of data

        - Args:
            executionDate: Execution Date.
            project: Name of the project.
            tablename: Name for table of path.

        - Return
            new_path_version
        """

        # Parse date
        executionDate = executionDate.split("-")

        # Partition data by Arguments
        year = executionDate[0]
        month = executionDate[1]
        day = executionDate[2]

        # Base path on HDFS
        base_path = f'hdfs://localhost:9000/lakehouse/LH_{project}/Files/{tablename}/year={year}/month={month}/day={day}'

        # Get all paths in HDFS
        hdfs_paths = os.popen(f"hadoop fs -ls {base_path}").read()

        # Define a regular expression pattern to match paths containing 'dfs' and 'parquet'
        pattern = r'hdfs:\/\/.*?\/{0}\/year=\d+\/month=\d+\/day=\d+\/{0}_\d+_\d+_\d+-version_(\d+)\.parquet'.format(re.escape(tablename))

        # Find all matches in the HDFS paths
        matches = re.findall(pattern, hdfs_paths)

        # Convert the version numbers to integers
        versions = [int(match) for match in matches]

        # Find the maximum version number and its index
        new_version = max(versions)

        # Get new path
        new_path_version = f'hdfs://localhost:9000/lakehouse/{project}/Files/{tablename}/year={year}/month={month}/day={day}/{tablename}_{year}_{month}_{day}-version_{new_version}.parquet'

        
        return new_path_version