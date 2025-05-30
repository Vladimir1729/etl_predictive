from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


def send_alert(message: str, task_id: str):
    return SlackWebhookOperator(
        task_id=task_id,
        slack_webhook_conn_id='slack_default',
        message=message,
        username='airflow',
        channel='#etl',
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

@task()
def start_alert():
    return send_alert(":rocket: El DAG (transfer-predictive) ha comenzado.", "start_alert").execute(context={})

@task()
def success_alert(result):
    message = f""":white_check_mark: El DAG (transfer-predictive) se completó exitosamente.
    Registros insertados: {result['rows_inserted']}."""
    return send_alert(message, "success_alert").execute(context={})

@task(trigger_rule=TriggerRule.ONE_FAILED)
def failure_alert():
    return send_alert(":red_circle: El DAG (transfer-predictive) ha fallado.", "failure_alert").execute(context={})

#