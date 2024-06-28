from airflow import DAG
# with operator need to set dag = dag for relationships
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.models import Variable

import sys
sys.path.append("/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source")

from modules.HadoopEcosystem import *
hadoop = HadoopEcosystem()

def create_data_pipeline_dag(hadoop, dagid):

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

    # Task run Source_to_Bronze job
    run_source_to_bronze = PapermillOperator(
        task_id = 'run_source_to_bronze_notebook',
        input_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_source_to_bronze.ipynb',
        output_nb = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/NB_output_phase_1.ipynb',
        dag = dag
    )


    # Task run Bronze_to_Silver job


    # Task to create Gold Table


    start_hadoop_task >> run_source_to_bronze

    # Return the created DAG
    return dag

# Create the DAG when the file is imported
data_pipeline_dag = create_data_pipeline_dag(hadoop, 'PL_Main_Global_Electronics_Retailer_New')
