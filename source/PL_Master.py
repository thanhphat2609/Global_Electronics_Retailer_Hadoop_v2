from airflow import DAG
# with operator need to set dag = dag for relationships
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.models import Variable

import sys
sys.path.append("/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source")

from modules.HadoopEcosystem import *
hadoop = HadoopEcosystem()

from modules.Metadata import *
metadata = Metadata()

def create_data_pipeline_dag(hadoop, metadata, dagid):

    # Create a DAG
    dag = DAG(
        f'{dagid}',
        description = 'A DAG run Pipeline Global Electronics Retailer',
        catchup = False
    )

    # Task to start Hadoop
    start_hadoop_task = PythonOperator(
        task_id = 'start_hadoop_service',
        python_callable = hadoop.start_hadoop,
        dag = dag,
    )

    # Task run find_job.py for finding Source_to_Bronze job in metadata
    set_source_to_bronze_job = PythonOperator(
        task_id = 'set_source_to_bronze_job',
        python_callable = metadata.read_metadata_action,
        op_kwargs = {'username': 'admin', 'password': 'admin', 'database': 'metadata', 'table': 'config_table', 'phase': 'CusDB -> Bronze'},
        dag = dag,
    )

    # Task run Source_to_Bronze job
    run_source_to_bronze = PapermillOperator(
        task_id = 'run_source_to_bronze_notebook',
        input_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_source_to_bronze.ipynb',
        output_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_output_phase_1.ipynb',
        dag = dag
    )


    # Task run Bronze_to_Silver job
    set_bronze_to_silver_job = PythonOperator(
        task_id = 'set_bronze_to_silver_job',
        python_callable = metadata.read_metadata_action,
        op_kwargs = {'username': 'admin', 'password': 'admin', 'database': 'metadata', 'table': 'config_table', 'phase': 'Bronze -> Silver'},
        dag = dag,
    )

    # Task run Bronze to Silver job
    run_bronze_to_silver= PapermillOperator(
        task_id = 'run_bronze_to_silver_notebook',
        input_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_bronze_to_silver.ipynb',
        output_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_output_phase_2.ipynb',
        dag = dag
    )

    # Task to create Gold Table
    # run_bronze_to_silver = PapermillOperator(
    #     task_id = 'run_bronze_to_silver_notebook',
    #     input_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_bronze_to_silver.ipynb',
    #     output_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_output_phase_2.ipynb',
    #     dag = dag
    # )


    start_hadoop_task >> set_source_to_bronze_job >> run_source_to_bronze \
    >> set_bronze_to_silver_job >> run_bronze_to_silver

    # Return the created DAG
    return dag

# Create the DAG when the file is imported
data_pipeline_dag = create_data_pipeline_dag(hadoop, metadata, 'PL_Main_Global_Electronics_Retailer_New')