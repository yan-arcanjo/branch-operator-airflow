import os
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Configurações Gerais
DAG_ID = 'BRANCH_CUBO_PEDIDOS'
TEMPLATE_SEARCH_PATH = f"{os.environ['SEARCH_PATH']}/{DAG_ID}/"
project_id = os.environ.get('GCP_PROJECT')
tz = pendulum.timezone('America/Sao_Paulo')


# Verifica primeira execução do dia para atualizar as dimensões
def check_first_execution(execution_date, **kwargs):
    local_time = execution_date.in_timezone('America/Sao_Paulo')
    if local_time.hour == 17:
        return 'start_dim_process'
    else:
        return 'skip_dim_process'

# Argumentos Padrão
default_args = {
    'owner': 'yan.arcanjo',
    'start_date': pendulum.datetime(year=2022, month=8, day=18).astimezone(tz),
    'email_on_failure': False,
    'email_on_success': False,
    'on_failure_callback': None,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}

# DAG Definition
with DAG(
    DAG_ID,
    description='Carga de dados BRANCH_CUBO_PEDIDOS',
    schedule_interval='0 1,12,15,17 * * *',
    default_args=default_args,
    catchup=False,
    tags=[
        'dim_Cliente', 'fato_Pedidos', 'fato_Notas'
    ],
    template_searchpath=[TEMPLATE_SEARCH_PATH],
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1
) as dag:
        
    begin = EmptyOperator(task_id='begin')

    # Branch para decidir se irá atualizar as dimensões
    branch = BranchPythonOperator(
        task_id='check_dim_execution',
        python_callable=check_first_execution,
        provide_context=True,
    )

    start_dim_process = EmptyOperator(task_id='start_dim_process', trigger_rule='all_done')

    # Task caso as dimensões não sejam executadas
    skip_dim_process = EmptyOperator(task_id='skip_dim_process')

    # Processamento dimensões
    insert_dim_clientes = BigQueryOperator(
                task_id='insert_dim_clientes',
                sql='sql_files/insert_dim_Clientes.sql',
                use_legacy_sql=False,
                gcp_conn_id='bigquery_default'
            )

    start_fato_process = EmptyOperator(task_id='start_fato_process', trigger_rule='all_done')

    # Processamento fatos
    insert_fato_pedidos = BigQueryOperator(
                task_id='insert_fato_pedidos',
                sql='sql_files/insert_fato_Pedidos.sql',
                use_legacy_sql=False,
                gcp_conn_id='bigquery_default'
            )
    
    insert_fato_notas = BigQueryOperator(
                task_id='insert_fato_notas',
                sql='sql_files/insert_fato_Notas.sql',
                use_legacy_sql=False,
                gcp_conn_id='bigquery_default'
            )
    
    end = EmptyOperator(task_id='end')

    # Orquestração
    begin >> branch

    branch >> skip_dim_process
    branch >> start_dim_process >> insert_dim_clientes
    
    insert_dim_clientes >> start_fato_process
    skip_dim_process >> start_fato_process

    start_fato_process >> [insert_fato_pedidos, insert_fato_notas] >> end