from airflow.sdk import dag,task
from airflow.operators import *
from datetime import datetime, timedelta

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=90)
}

@dag(
    dag_id="weather_alert_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False, 
    default_args=default_args,
)

def weather_alert_dag():

    @task.python
    def extract_weather_data(**kwargs):
        from src.weather_alert._01_extract_weather_data import fetch_weather_batch
        output=fetch_weather_batch()
        ti=kwargs['ti']
        ti.xcom_push(key='fetch_weather_batch',value=output)
        return output
    
    @task.python
    def clean_weather_data(**kwargs):
        from src.weather_alert._02_clean_weather_data import clean_weather_data
        ti=kwargs['ti']
        input=ti.xcom_pull(key='fetch_weather_batch',task_ids='extract_weather_data')
        output=clean_weather_data(input)
        ti.xcom_push(key='cleaned_weather_data',value=output)
        return output
    
    
    @task.python
    def transform_weather_data(**kwargs):
        from src.weather_alert._03_transform_weather_data import tranform_weather_data
        ti=kwargs['ti']
        input=ti.xcom_pull(key='cleaned_weather_data',task_ids='clean_weather_data')
        output=tranform_weather_data(input)
        ti.xcom_push(key='transformed_weather_data',value=output)
        return output
    
    @task.python
    def validate_weather_data(**kwargs):
        from src.weather_alert._04_validate_weather_data import validate
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_weather_data',task_ids='transform_weather_data')
        output=validate(input)
        ti.xcom_push(key='validate_weather_data',value=output)
        return output
    
    @task.branch
    def validation_checker(**kwargs):
        from src.weather_alert._04_validate_weather_data import validate
        ti=kwargs['ti']
        validation_result=ti.xcom_pull(key='validate_weather_data',task_ids='validate_weather_data')
        if validation_result is True:
            return ["monitor_weather_data","load_weather_data"]
        else:
            return "raiserror"
        
    @task.python
    def raiserror(**kwargs):
        return "❌ Execution stopped due to validation failure."
    
    @task.python
    def error_alert():
        from src.weather_alert._09_pipeline_error_alert import send_pipeline_halted_email
        output=send_pipeline_halted_email()
        return output
    
    @task.python
    def monitor_weather_data(**kwargs):
        from src.weather_alert._06_monitor_weather_data import monitor_weather_events
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_weather_data',task_ids='transform_weather_data')
        output=monitor_weather_events(input)
        if hasattr(output, 'select_dtypes'):
            for col in output.select_dtypes(include=['datetime64']).columns:
                output[col] = output[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        ti.xcom_push(key='monitor_weather_data',value=output)
        return output
    
    
    @task.python
    def alert_weather_data(**kwargs):
        from src.weather_alert._07_generate_alert_weather_data import send_weather_threat_alert_email
        from dotenv import load_dotenv
        import os
        load_dotenv()
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_weather_data',task_ids='transform_weather_data')
        alerts=ti.xcom_pull(key='monitor_weather_data',task_ids='monitor_weather_data')
        recipient_emails = os.getenv("RECIPIENT_EMAILS", "")
        # for users
        # recipient_emails=os.getenv("USER_RECIPIENT_EMAILS", "")
        recipient_list = [email.strip() for email in recipient_emails.split(",") if email.strip()]
        recipient_email=recipient_list
        sender_email=os.getenv("SENDER_MAIL_ID")
        sender_password=os.getenv("GMAIL_APP_PASSWORD")
        output=send_weather_threat_alert_email(alerts,recipient_email, sender_email, sender_password)

    @task.python
    def load_weather_data(**kwargs):
        from src.weather_alert._05_load_data_to_db import load_dataframe_to_postgres
        import pandas as pd
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_weather_data',task_ids='transform_weather_data')
        output=load_dataframe_to_postgres(input)
        ti.xcom_push(key='load_weather_data',value=output)
        return output
    
    @task.python
    def data_load_alert(**kwargs):
        from src.weather_alert._08_data_load_alert import send_load_success_email
        ti=kwargs['ti'] # You can also make this dynamic by counting the records loaded
        load_status=ti.xcom_pull(key='load_weather_data',task_ids='load_weather_data')
        is_skipped = 'no'
        is_failure = 'no'
        schema_name = table_name = batch_id = record_count = None
        if load_status[0]==True and len(load_status)==1:
            is_skipped='yes'
        if load_status[0]==True and len(load_status)>1:
            schema_name = load_status[1]
            table_name = load_status[2]
            batch_id = load_status[3]
            record_count = load_status[4]
        if load_status[0]==False:
            is_failure='yes'
    
            
        send_load_success_email(is_skipped,is_failure,schema_name,table_name,batch_id,record_count)
    
    
    step1_extract=extract_weather_data()
    step2_clean=clean_weather_data()
    step3_transform=transform_weather_data()
    step4_validate=validate_weather_data()
    step5_validation_check=validation_checker()
    step6_raiserror=raiserror()
    step6_load=load_weather_data()
    step7_monitor=monitor_weather_data()
    step8_alert=alert_weather_data()
    step9_data_load_alert=data_load_alert()
    step10_pipeline_error_alert=error_alert()
    
    
    step1_extract >> step2_clean >> step3_transform >> step4_validate >> step5_validation_check >> [step6_load,step6_raiserror]
    
    step6_raiserror >> step10_pipeline_error_alert
     
    step5_validation_check >> step7_monitor >> step8_alert
    
    step6_load >> step9_data_load_alert
weather_alert_dag()