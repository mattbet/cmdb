# dags/vcloud_to_postgres_dag.py
from datetime import datetime, timedelta
import json  # <-- AGGIUNTO PER GESTIRE I DIZIONARI
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

# Importiamo la funzione e la URL dal modulo comune
from DADC4A14445_refresh import vcloud_login_refresh, VCLOUD_URL

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clean_json_field(field_value):
    """Utility per convertire strutture complesse (dict, list) in stringhe JSON prima del DB"""
    if isinstance(field_value, (dict, list)):
        return json.dumps(field_value)
    return field_value

def estrai_e_carica_vms(**kwargs):
    ti = kwargs['ti']
    # Riceve il token passato da DADC4A14445_refresh
    token = ti.xcom_pull(task_ids='refresh_vcloud_token')
    
    auth_headers = {
        "Accept": "application/*+json;version=38.0",
        "Authorization": f"Bearer {token}" 
    }

    query_url = f"{VCLOUD_URL}/api/query?type=vm"
    print("📡 Estrazione dati VM tramite token di Console...")
    response = requests.get(query_url, headers=auth_headers, verify=False, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"Errore API VM: {response.status_code} - {response.text}")
        
    records = response.json().get('record', [])
    if not records:
        print("Nessuna VM trovata.")
        return

    orario_inserimento = timezone.utcnow()
    dati_da_inserire = [
         (
             item.get('href').split('/')[-1] if item.get('href') else None, 
             item.get('_type'), 
             clean_json_field(item.get('link')),          # <-- APPLICATO JSON CLEANING
             clean_json_field(item.get('metadata')),      # <-- APPLICATO JSON CLEANING
             item.get('href'), 
             item.get('id'), 
             item.get('type'), 
             clean_json_field(item.get('otherAttributes')),# <-- APPLICATO JSON CLEANING
             item.get('name'), 
             item.get('containerName'), 
             item.get('container'), 
             item.get('ownerName'), 
             item.get('owner'), 
             item.get('vdcName'), 
             item.get('vdc'), 
             item.get('description'), 
             item.get('vappScopedLocalId'), 
             item.get('isVAppTemplate'), 
             item.get('isDeleted'), 
             item.get('guestOs'), 
             item.get('detectedGuestOs'), 
             item.get('numberOfCpus'), 
             item.get('memoryMB'), 
             item.get('status'), 
             item.get('networkName'), 
             item.get('network'), 
             item.get('ipAddress'), 
             item.get('isBusy'), 
             item.get('isDeployed'), 
             item.get('isPublished'), 
             item.get('catalogName'), 
             item.get('hardwareVersion'), 
             item.get('vmToolsStatus'), 
             item.get('isInMaintenanceMode'), 
             item.get('isAutoNature'), 
             item.get('storageProfileName'), 
             clean_json_field(item.get('snapshot')),      # <-- APPLICATO JSON CLEANING
             item.get('snapshotCreated'), 
             item.get('gcStatus'), 
             item.get('autoUndeployDate'), 
             item.get('autoDeleteDate'), 
             item.get('isAutoUndeployNotified'), 
             item.get('isAutoDeleteNotified'), 
             item.get('isComputePolicyCompliant'), 
             item.get('vmSizingPolicyId'), 
             item.get('vmPlacementPolicyId'), 
             item.get('encrypted'), 
             item.get('dateCreated'), 
             item.get('totalStorageAllocatedMb'), 
             item.get('isExpired'), 
             item.get('defaultStoragePolicyName'), 
             item.get('hasVgpuPolicy'), 
             item.get('firmware'), 
             item.get('tpmPresent'), 
             item.get('replicationState'), 
             orario_inserimento
         )
        for item in records
    ]

    # NOTA: Assicurati di usare 'postgres_airflow' che hai creato sui pannelli Airflow!
    pg_hook = PostgresHook(postgres_conn_id='postgres_airflow')
    pg_hook.insert_rows(
        table='vcloud_vms_report', 
        rows=dati_da_inserire, 
        target_fields=[
            'vcloud_extracted_id', '_type', 'link', 'metadata', 'href', 'id', 'type', 
            'other_attributes', 'name', 'container_name', 'container', 'owner_name', 
            'owner', 'vdc_name', 'vdc', 'description', 'vapp_scoped_local_id', 
            'is_vapp_template', 'is_deleted', 'guest_os', 'detected_guest_os', 
            'number_of_cpus', 'memory_mb', 'status', 'network_name', 'network', 
            'ip_address', 'is_busy', 'is_deployed', 'is_published', 'catalog_name', 
            'hardware_version', 'vm_tools_status', 'is_in_maintenance_mode', 
            'is_auto_nature', 'storage_profile_name', 'snapshot', 'snapshot_created', 
            'gc_status', 'auto_undeploy_date', 'auto_delete_date', 'is_auto_undeploy_notified', 
            'is_auto_delete_notified', 'is_compute_policy_compliant', 'vm_sizing_policy_id', 
            'vm_placement_policy_id', 'encrypted', 'date_created', 'total_storage_allocated_mb', 
            'is_expired', 'default_storage_policy_name', 'has_vgpu_policy', 'firmware', 
            'tpm_present', 'replication_state', 'data_inserimento'
        ]
    )
    print("✅ Database Postgres aggiornato.")

with DAG(
    dag_id='vcloud_pipeline_token_console',
    default_args=default_args,
#    schedule_interval='@daily',
    schedule_interval=None,  # Disattiva la pianificazione automatica,
    catchup=False,
    max_active_runs=1
) as dag:

    task_refresh_token = PythonOperator(
        task_id='refresh_vcloud_token',
        python_callable=vcloud_login_refresh,
        provide_context=True
    )

    task_vms = PythonOperator(
        task_id='estrai_vms_to_postgres',
        python_callable=estrai_e_carica_vms,
        provide_context=True
    )

    task_refresh_token >> task_vms
