from pyspark.sql import SparkSession

import traceback
import pyspark.sql.functions as f
import pyspark.sql.types as t

from modules.Extraction import *
from modules.HDFSUtils import *
from modules.LogUtils import *


# Instance for modules
extraction = Extraction()
hdfsUtils = HDFSUtils()
logUtils = LogUtils() 

# Create SparkSession
spark = SparkSession.builder.appName("Source_to_Bronze") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", 100) \
                            .getOrCreate()


project_name = "Global_Electronics_Retailer"

executionDate = str(spark.sql("SELECT CURRENT_DATE()").collect()[0][0])
# print(exuctionDate)

# Partition Execution Date
parse_execution = executionDate.split("-")
year = parse_execution[0]
month = parse_execution[1]
day = parse_execution[2]


# Define base_path
file_path = f"hdfs://localhost:9000/lakehouse/LH_{project_name}/Files/Bronze"
log_path = f"hdfs://localhost:9000/lakehouse/LH_{project_name}/Files/log"


# Define for log job
batch_run = hdfsUtils.check_batch_run(project_name, executionDate)
start_time = ""
end_time = ""
error = ""
status = ""
source_row_read = 0
numInserted = 0
numUpdated = 0
pipeline_job = "source_to_bronze"


# Define parameter for connect to MySQL
database = "Global_Electronics_Retailer"
dbname = f"jdbc:mysql://localhost:3306/{database}"
driver = "com.mysql.jdbc.Driver"
username = "root"
password = "password"

# Get table for source check
check_source = "Source_Check"
df_check_source = extraction.read_table_mysql(spark, driver, dbname, check_source, username, password)
# df_check_source.show()


# tblNames
tblNames = ["customers", "sales", "products", "stores", "exchange_rates"]


# # Read all table
for tblName in tblNames:
    # Start time for check
    start_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')
    try:
        # Read data
        df = extraction.read_table_mysql(spark, driver, dbname, tblName, username, password)
        

        # Validate data
        source_row_read = df_check_source.filter(f.col("table_name") == f"{tblName}").select("source_row_read").collect()[0][0]
        numInserted = df.count()

        # Create new column for partition
        df = extraction.create_year_month_day(df, executionDate, f)
        
        # Display df
        # df.show()

        # Write data to HDFS
        code = hdfsUtils.check_exist_data(executionDate, project_name, tblName)
        # Exist file
        if code == 0: # Yes => Append for version data
            df.write.mode("append").format("parquet") \
                    .save(f"{file_path}/{tblName}/year={year}/month={month}/day={day}/{tblName}_{year}_{month}_{day}-version_{batch_run}.parquet")
        else: # No => First run
            df.write.mode("overwrite").format("parquet") \
                    .save(f"{file_path}/{tblName}/year={year}/month={month}/day={day}/{tblName}_{year}_{month}_{day}-version_{batch_run}.parquet")
    
    except:
        error = traceback.format_exc()
        status = "Failed"

    else:
        error = ""
        status = "Success"
    
    # End time for check
    end_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')

    # Check status
    # print("Tablename: ", tblName, "Error: ", error, "Status: ", status, 
    #       "Source rows: ", source_row_read, "Num of rows Inserted: ", numInserted)


    df_log = logUtils.log_data(batch_run, pipeline_job, database, tblName,
                 start_time, end_time, source_row_read, numInserted, numUpdated, "", 
                 "", error, status, t, spark)

    df_log.write.mode("append").format("parquet").save(f"{log_path}/{executionDate}/batch_{batch_run}/")


