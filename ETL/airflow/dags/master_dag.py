from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

# Definizione del DAG principale (Controllore)
with DAG(
    dag_id="dag_controllore_principale",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Esecuzione manuale o su scheduler
    catchup=False,
    tags=["orchestration"],
) as dag:

    inizio = EmptyOperator(task_id="inizio")

    # 1. Trigger del primo DAG con verifica del risultato
    trigger_dag_A = TriggerDagRunOperator(
        task_id="avvia_e_verifica_dag_A",
        trigger_dag_id="id_del_tuo_dag_figlio_A",  # Sostituisci con l'ID reale del DAG A
        wait_for_completion=True,                 # Mantiene il task attivo finché il DAG A non finisce
        poke_interval=30,                         # Controlla lo stato del DAG figlio ogni 30 secondi
        allowed_states=["success"],               # Il task prosegue SOLO se il DAG figlio va in "success"
        failed_states=["failed", "upstream_failed"], # Se il DAG figlio fallisce, questo task fallisce subito
    )

    # 2. Trigger del secondo DAG (eseguito solo se il DAG A ha successo)
    trigger_dag_B = TriggerDagRunOperator(
        task_id="avvia_e_verifica_dag_B",
        trigger_dag_id="id_del_tuo_dag_figlio_B",  # Sostituisci con l'ID reale del DAG B
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    fine = EmptyOperator(task_id="fine")

    # Flusso di dipendenze sequenziale
    inizio >> trigger_dag_A >> trigger_dag_B >> fine
