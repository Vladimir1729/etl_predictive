from airflow.decorators import dag, task
from datetime import datetime, timedelta
from slack_alerts import start_alert, success_alert, failure_alert
from data_transfer_etlsql import extract_and_load_data




#Dag principal
@dag(
    dag_id='heartbeat_predictive',
    schedule_interval = None,
    start_date=datetime(2024, 8, 26),
    catchup=False,
    tags=['transfer']
)

def transfer_general():
    #Definimos las tareas
    start_alert_task = start_alert()
    load_result = extract_and_load_data()
    success_alert_task = success_alert(load_result)
    failure_alert_task = failure_alert()

    #definimos el flujo
    start_alert_task >> load_result
    load_result >> [success_alert_task, failure_alert_task]

dag = transfer_general()