# End to End project Global Electronics Retailer Analysis with Hadoop Ecosystem using Lakehouse(Medallion Architecture).

_Table of contents_
- [**1. Hadoop Ecosystem**](#1-hadoop-ecosystem)
- [**2. Data Architecture**](#2-data-architecture)
  * [2.1. Conceptual Architecture base on Fabric](#21-conceptual-architecture-base-on-fabric)
  * [2.2. Physical Architecture](#22-physical-architecture)
- [**3. Building End to End solutions**](#3-building-end-to-end-solutions)
  * [3.1. Dataset Diagram](#31-dataset-diagram)
  * [3.2. Building HDFS](#32-building-hdfs)
  * [3.3. Building Orchestration](#33-building-orchestration)
- [**4. Result**](#4-result)
  * [4.1. Pipeline](#41-pipeline)
  * [4.2. Lakehouse](#42-lakehouse)
 

# **1. Hadoop Lakehouse Ecosystem**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/56d37d82-6ca3-4c76-af90-90ab3be186fe)

Introduce some tools for project:

- **Hadoop**: Hadoop is an open-source framework designed for distributed storage and processing of large datasets across clusters of computers using simple programming models. It provides a distributed file system (HDFS) and a framework for the processing of big data using the MapReduce programming model.

- **HDFS** (Hadoop Distributed File System): HDFS is a distributed file system designed to store large volumes of data reliably and efficiently across multiple machines. It is the primary storage system used by Hadoop, providing high throughput access to application data.

- **Apache Spark**: Apache Spark is an open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Spark's in-memory computing capabilities make it well-suited for iterative algorithms and interactive data analysis.

- **Apache Hive**: Hive is a data warehouse infrastructure built on top of Hadoop that provides data summarization, query, and analysis. It enables querying and managing large datasets stored in Hadoop's HDFS using a SQL-like language called HiveQL.

- **Apache Superset**: Apache Superset is an open-source business intelligence (BI) tool that offers a rich set of visualization options and features for exploring and analyzing data. It supports a wide range of data sources and allows users to create interactive dashboards and data exploration workflows.

- **Delta Lake**: Delta Lake is an open-source storage layer that brings reliability to data lakes. Similar to Hadoop and HDFS, which provide distributed storage and processing capabilities, Delta Lake enhances the reliability and performance of large-scale data processing and analytics.


# **2. Data Architecture**

## 2.1. Conceptual Architecture base on Hadoop Ecosystem
![Items (1)](https://github.com/thanhphat2609/Global_Super_Store/assets/84914537/600e237e-01d7-4c09-891c-1551acfbc45e)

- **Data Source**: These include the various systems from which data is **extracted**, such as: Relational Database, File systems, SaaS applications, Real-time data.
- **Staging**: Extract data from Source into Files of Datalake (csv, parquet).
- **Data Warehouse**: Data in the data warehouse is organized according to a unified data model, which makes it easy to query and analyze.
- **Analytics**: This last step we will use tools and techniques to analyze the data in the data warehouse, such as: Power BI, Tableau, ..

## 2.2. Physical Architecture
![Hadoop_Architecture_Lakehouse](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/141325ba-bd95-4bde-b7b5-1f64923ae2c1)


## 3.1. Dataset Diagram
![DataSetDiagram](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop/assets/84914537/e34766d2-8b75-4e32-8445-7bc4dcbd610e)

**Data_Dictionary**
[Global_Electronics_Retailer_Data_Dictonary](https://docs.google.com/spreadsheets/d/149kBQERsr9I5RbcBwBhVJdaATtK1lOVu/edit?usp=sharing&ouid=104868242064941170355&rtpof=true&sd=true)


## 3.2. Building HDFS
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/763fb1b7-c735-4fff-b40a-d454368b8dc1)


## 3.3. Building Orchestration
- **Pipeline Airflow**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/048b1769-5045-4ba2-bbd9-35c146fa1cc9)

- **Metadata in MongoDB**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/9734af75-915f-4a2e-9cc2-7db26dad6ea5)


- **Notebook, Pythongs run ETL**

| **Python Files**          | **Meaning** |
|-------------------|-------------- |
| NB_source_to_bronze.ipynb | Notebook for extract data from MySQL and load to Files in Lakehouse |
| NB_output_phase_1.ipynb | Notebook store result for phase 1 (CusDB -> Bronze) |
| NB_bronze_to_silver.ipynb | Notebook for transformation bronze data from Files and load to Silver as Delta |
| NB_output_phase_2.ipynb | Notebook store result for phase 2 (Bronze -> Silver) |
| NB_gold_create_delta.ipynb | Notebook crete delta table for Gold layer for Analytics |
| PL_Master.py | Define task for Airflow run |


- **Modules define by Python**

| **Python Files**          | **Meaning** |
|-------------------|-------------- |
| Extraction.py | Class Extraction for extract data |
| Transformation.py | Class Transformation for check update, insert, transformation data |
| Load.py | Class Load to load data to table, check exist table in Hive |
| HadoopEcosystem.py | Class HadoopEcosystem for start service of Hadoop eco |
| HDFSUtils.py | Class HDFSUtils for HDFS in Hadoop |
| LogUtils.py | Class LogUtils pipeline and task |
| Metadata.py | Class connect and read metadata table from MongoDB for run ETL |

# **4. Result**

## 4.1. Pipeline
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/69f93519-01e2-4aa8-8449-3019c631bb6e)

## 4.2 Lakehouse
- **Bronze**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/7fd23370-53e7-4a35-8448-9f616558719d)

- **Silver**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/c36afe2c-9902-4db0-a661-10f13879a4d3)


- **Log**
**HDFS store**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/9e404ef3-ee5d-4013-b0da-ba0d9136c311)

**Result**
![image](https://github.com/thanhphat2609/Global_Electronics_Retailer_Hadoop_v2/assets/84914537/552e0aa2-64a6-4362-a03f-1dfe3abb8468)


- **Gold**

Not updated yet.
