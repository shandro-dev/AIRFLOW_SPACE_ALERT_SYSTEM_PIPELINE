from airflow.sdk import dag,task
from airflow.operators import *
from datetime import datetime, timedelta

@dag(
    dag_id="space_alert_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False, 
)

def space_alert_dag():

    @task.python
    def extract_neo_data(**kwargs):
        from src.space_alert.step01_extract_neo_data import neosapi
        output=neosapi()
        ti=kwargs['ti']
        ti.xcom_push(key='raw_neo_data',value=output)
        return output
    
    @task.python
    def clean_neo_data(**kwargs):
        from src.space_alert.step02_clean_neo_data import clean_data
        ti=kwargs['ti']
        input=ti.xcom_pull(key='raw_neo_data',task_ids='extract_neo_data')
        output=clean_data(input)
        ti.xcom_push(key='cleaned_neo_data',value=output)
        return output
    
    
    @task.python
    def transform_neo_data(**kwargs):
        from src.space_alert.step03_transform_neo_data import transform_data
        ti=kwargs['ti']
        input=ti.xcom_pull(key='cleaned_neo_data',task_ids='clean_neo_data')
        output=transform_data(input)
        ti.xcom_push(key='transformed_neo_data',value=output)
        return output
    
    @task.python
    def validate_neo_data(**kwargs):
        from src.space_alert.step04_validate_neo_data import validate_data
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_neo_data',task_ids='transform_neo_data')
        output=validate_data(input)
        ti.xcom_push(key='validate_neo_data',value=output)
        return output
    
    @task.branch
    def validation_checker(**kwargs):
        from src.space_alert.step04_validate_neo_data import validate_data
        ti=kwargs['ti']
        validation_result=ti.xcom_pull(key='validate_neo_data',task_ids='validate_neo_data')
        if validation_result==True:
            return ["monitor_neo_data","load_neo_data"]
        else:
            return "raiserror"
        
    @task.python
    def raiserror(**kwargs):
        return "❌ Execution stopped due to validation failure."
    
    @task.python
    def error_alert():
        from src.space_alert.step09_pipeline_error_alert import send_pipeline_halted_email
        output=send_pipeline_halted_email()
        return output
    
    @task.python
    def monitor_neo_data(**kwargs):
        from src.space_alert.step06_monitor_neo_data import monitor_neo
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_neo_data',task_ids='transform_neo_data')
        output=monitor_neo(input)
        if hasattr(output, 'select_dtypes'):
            for col in output.select_dtypes(include=['datetime64']).columns:
                output[col] = output[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        ti.xcom_push(key='monitor_neo_data',value=output)
        return output
    
    
    @task.python
    def alert_neo_data(**kwargs):
        from src.space_alert.step07_generate_alert_neo_data import send_asteroid_threat_alert_email
        from dotenv import load_dotenv
        import os
        load_dotenv()
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_neo_data',task_ids='transform_neo_data')
        alerts=ti.xcom_pull(key='monitor_neo_data',task_ids='monitor_neo_data')
        recipient_emails = os.getenv("RECIPIENT_EMAILS", "")
        # for users
        # recipient_emails=os.getenv("USER_RECIPIENT_EMAILS", "")
        recipient_list = [email.strip() for email in recipient_emails.split(",") if email.strip()]
        recipient_email=recipient_list
        sender_email=os.getenv("SENDER_MAIL_ID")
        sender_password=os.getenv("GMAIL_APP_PASSWORD")
        output=send_asteroid_threat_alert_email(alerts,recipient_email, sender_email, sender_password)

    @task.python
    def load_neo_data(**kwargs):
        from src.space_alert.step05_load_neo_data import load_dataframe_to_postgres
        import pandas as pd
        ti=kwargs['ti']
        input=ti.xcom_pull(key='transformed_neo_data',task_ids='transform_neo_data')
        output=load_dataframe_to_postgres(input)
        ti.xcom_push(key='load_neo_data',value=output)
        return output
    
    @task.python
    def data_load_alert(**kwargs):
        from src.space_alert.step08_data_load_alert import send_load_success_email
        ti=kwargs['ti'] # You can also make this dynamic by counting the records loaded
        load_status=ti.xcom_pull(key='load_neo_data',task_ids='load_neo_data')
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
    
    
    step1_extract=extract_neo_data()
    step2_clean=clean_neo_data()
    step3_transform=transform_neo_data()
    step4_validate=validate_neo_data()
    step5_validation_check=validation_checker()
    step6_raiserror=raiserror()
    step6_load=load_neo_data()
    step7_monitor=monitor_neo_data()
    step8_alert=alert_neo_data()
    step9_data_load_alert=data_load_alert()
    step10_pipeline_error_alert=error_alert()
    
    
    step1_extract >> step2_clean >> step3_transform >> step4_validate >> step5_validation_check >> [step6_load,step6_raiserror]
    
    step6_raiserror >> step10_pipeline_error_alert
     
    step5_validation_check >> step7_monitor >> step8_alert
    
    step6_load >> step9_data_load_alert
space_alert_dag()