class Load:
    def __init__(self) -> None:
        pass

    def writeInit(self, df, spark, lakehouse_table_path, lakewarehouse_db, 
                table_name):
        
        """
        Function Write Initial to HDFS by Spark connect Hive

        - Args:
            df: Dataframe
            spark: SparkSession
            lakehouse_table_path: Lakehouse table path 
            lakewarehouse_db: Warehouse in Lakehouse
            table_name: Delta table name in Lakehouse

        - Return
            None
        """

        # Write to lakehouse
        df.write.format("delta").mode("overwrite").save(f"{lakehouse_table_path}/{table_name}")

        spark.sql(f"USE {lakewarehouse_db};")
        spark.sql(f"""CREATE TABLE {table_name} USING DELTA LOCATION '{lakehouse_table_path}/{table_name}';""")