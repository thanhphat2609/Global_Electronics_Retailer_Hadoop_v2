import sys
import subprocess


project_name = sys.argv[1]
# print(project_name)


# Create folder for project
command = f"mkdir /home/thanhphat/PersonalProject/{project_name}"
subprocess.run(command, shell=True, capture_output=True, text=True)

# Create source
command = f"mkdir /home/thanhphat/PersonalProject/{project_name}/source"
subprocess.run(command, shell=True, capture_output=True, text=True)

# Start hadoop
command = "start-all.sh"
subprocess.run(command, shell=True, capture_output=True, text=True)


# Create lakehouse for project
command = f"hdfs dfs -mkdir /lakehouse/LH_{project_name}"
subprocess.run(command, shell=True, capture_output=True, text=True)

# Datalake for Lakehouse
command = f"hdfs dfs -mkdir /lakehouse/LH_{project_name}/Files"
subprocess.run(command, shell=True, capture_output=True, text=True)
# Warehouse for Lakehouse
command = f"hdfs dfs -mkdir /lakehouse/LH_{project_name}/Tables"
subprocess.run(command, shell=True, capture_output=True, text=True)

# Create warehouse for project
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Create Warehouse Metadata") \
                    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/lakehouse/warehouse") \
                    .config("spark.sql.catalogImplementation", "hive") \
                    .enableHiveSupport() \
                    .getOrCreate()

spark.sql(f"CREATE DATABASE LH_{project_name}")


spark.stop 


