import pandas as pd
import numpy as np
import os
import gc
import math
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


@task()
def extract_and_load_data():

    #Rango de tiempo de los ultimos 10 minutos
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes = 10)


    #Convertimos a string con formato sql
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')


    print(f'Analizando IDs entre {start_time_str} Y {end_time_str}')

    #yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


    #Hooks para conexiones
    source_hook = MsSqlHook(mssql_conn_id = '')
    target_hook = MsSqlHook(mssql_conn_id = '')
    

    #Consultar el rango de IDs del dia anterior
    id_query = f"""
        SELECT 
            MIN(t.id) AS min_id,
            MAX(t.id) AS max_id
        FROM transactions_history t
        WHERE t.date BETWEEN '{start_time_str}' AND '{end_time_str}'
    """

    result = source_hook.get_first(id_query)

    if result is None or not all(result):
        print(f'No se encontraron datos entre {start_time_str} y {end_time_str}')
        return {{'rows_inserted': 0}}
    

    min_id, max_id = result
    print(f'Rango de IDs en ventana de 10 minutos: {min_id} - {max_id}')


    #Definir la lista de intervalos de ID
    num_intervals = 1
    interval_size = math.ceil((max_id - min_id + 1) / num_intervals)
    id_intervals = [(min_id + i * interval_size, min(min_id + (i + 1) * interval_size - 1, max_id)) for i in range(num_intervals)]
    print(f'Intervalos generados automáticamente: {id_intervals}')

    #Leer las consultas SQL
    dag_dir = os.path.dirname(os.path.realpath(__file__))
    count_query_path = os.path.join(dag_dir, 'count_query_etlsql.sql')
    paginated_query_path = os.path.join(dag_dir, 'paginated_query_etlsql.sql')

    with open(count_query_path, 'r') as file:
        count_query_template = file.read()

    with open(paginated_query_path, 'r') as file:
        paginated_query_template = file.read()

    rows_inserted = 0

    for interval in id_intervals:
        start_id, end_id = interval
        count_query = count_query_template.format(start_id=start_id, end_id=end_id)
        total_rows = source_hook.get_first(count_query)[0]
        rows_per_page = math.ceil(total_rows * 0.10)
        page_number = 1
        fetched_rows = rows_per_page

        while fetched_rows == rows_per_page:
            paginated_query = paginated_query_template.format(
                page_number=page_number,
                rows_per_page=rows_per_page,
                start_id=start_id,
                end_id=end_id
            )
            df = source_hook.get_pandas_df(paginated_query)
            fetched_rows = df.shape[0]
            
            if fetched_rows > 0:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')

                # CARGA A OTRA BASE SQL SERVER
                target_conn = target_hook.get_sqlalchemy_engine()
                df.to_sql(
                    'transactions_history_destino',   # <- Nombre de tabla en destino
                    con=target_conn,
                    index=False,
                    if_exists='append'
                )
                rows_inserted += df.shape[0]
                print(f'Datos insertados en destino: {rows_inserted} filas')

            page_number += 1

    gc.collect()
    return {'rows_inserted': rows_inserted}








