from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Import delle funzioni dallo script psn_baas_token_renew.py.py
from psn_baas_token_renew.py import check_token_status, renew_and_store_tokens, get_virtual_machines

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def decide_next_step():
    is_valid = check_token_status()
    if is_valid:
        return 'skip_renewal'
    else:
        return 'renew_tokens_task'

with DAG(
    'psn_baas_query_vm_dag',
    default_args=default_args,
    description='Gestisce il ciclo dei token BaaS e interroga l API Virtual Machines',
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['baas', 'auth', 'vms'],
) as dag:

    # 1. Bivio di controllo validità token
    check_tokens = BranchPythonOperator(
        task_id='check_token_validity',
        python_callable=decide_next_step,
    )

    # 2. Task di Rinnovo (se scaduto)
    renew_tokens = PythonOperator(
        task_id='renew_tokens_task',
        python_callable=renew_and_store_tokens,
    )

    # 3. Task di Skip (se valido)
    skip_renewal = EmptyOperator(
        task_id='skip_renewal',
    )

    # 4. Task di interrogazione elenco VM (eseguito sempre alla fine)
    fetch_vms = PythonOperator(
        task_id='fetch_virtual_machines',
        python_callable=get_virtual_machines,
        trigger_rule=TriggerRule.ONE_SUCCESS, # Esegue se almeno uno dei due rami precedenti ha successo
    )

    # 5. Task di interrogazione elenco VM (eseguito sempre alla fine)
WITH api_response AS (
    SELECT :json_payload::jsonb AS payload
)
INSERT INTO commvault_virtual_machines (
    uuid,
    name,
    display_name,
    vendor,
    description,
    cloud_vendor,
    region_name,
    os,
    host,
    vm_size,
    application_size,
    status,
    hypervisor_id,
    hypervisor_name,
    vm_group_id,
    vm_group_name,
    backup_set_id,
    backup_set_name,
    commcell_name,
    last_backup_time,
    last_backup_job_id,
    last_backup_status,
    last_successful_backup_time,
    last_backup_failure_reason,
    latest_recovery_point,
    oldest_recovery_point,
    plan_id,
    plan_name,
    plan_subtype,
    sla_status,
    sla_reason,
    company_id,
    company_name,
    raw_data
)
SELECT
    (vm->>'UUID')::uuid,
    vm->>'name',
    vm->>'displayName',
    vm->>'vendor',
    vm->>'description',
    vm->>'cloudVendor',
    vm->>'regionName',
    trim(vm->>'os'),
    vm->>'host',
    (vm->>'vmSize')::bigint,
    (vm->>'applicationSize')::bigint,
    vm->>'status',
    (vm->'hypervisor'->>'id')::bigint,
    vm->'hypervisor'->>'name',
    (vm->'vmGroup'->>'id')::bigint,
    vm->'vmGroup'->>'name',
    (vm->'backupset'->>'backupSetId')::bigint,
    vm->'backupset'->>'backupSetName',
    vm->>'commcellName',
    to_timestamp((vm->'lastBackup'->>'time')::bigint),
    (vm->'lastBackup'->>'jobId')::bigint,
    vm->'lastBackup'->>'status',
    to_timestamp(
        (vm->'lastBackup'->>'lastSuccessfullBackupTime')::bigint
    ),
    vm->'lastBackup'->>'failureReason',
    to_timestamp((vm->>'latestRecoveryPoint')::bigint),
    to_timestamp((vm->>'oldestRecoveryPoint')::bigint),
    (vm->'plan'->>'id')::bigint,
    vm->'plan'->>'name',
    vm->'plan'->>'subType',
    vm->'SLA'->>'status',
    vm->'SLA'->>'reason',
    (vm->'company'->>'id')::bigint,
    vm->'company'->>'name',
    vm
FROM api_response
CROSS JOIN LATERAL
    jsonb_array_elements(payload->'virtualMachines') AS vm;


    # Definizione delle dipendenze del flusso
    check_tokens >> [renew_tokens, skip_renewal]
    [renew_tokens, skip_renewal] >> fetch_vms
