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
  * [4.2. Datalake](#42-datalake)
  * [4.3. Data Warehouse](#43-data-warehouse)
  * [4.4. Log Pipeline and Data](#44-log-pipeline-and-data)
  * [4.5. Superset Report](#45-superset-report)
 

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
