import asyncio
from typing import Literal

from fastapi import APIRouter, Query

from configuration import keyvault
from service.airflow_service import (get_dag_status_from_id, async_trigger_dag,
                                     get_dag_logs_from_run_id, get_query_response)

airflow_router = APIRouter(prefix="/airflow2/api/v1",
                           tags=["Airflow APIs (environment dependant)"], )

env_list = [key for key in keyvault.keys() if
            isinstance(keyvault[key], dict) and keyvault[key].get("data_partition_id") is not None]

data_partition_list = set()
for key in keyvault.keys():
    if isinstance(keyvault[key], dict) and keyvault[key].get("data_partition_id") not in [None,""]:
        data_partition_list.add(*keyvault.get(key).get("data_partitions"))

dag_list = keyvault['dags-ltops'] + keyvault['dags-seismic-ltops']

@airflow_router.get("/dags/run_status")
def get_dag_run_status(env: Literal[*env_list] = Query(...),
                       dag: Literal[*dag_list] = Query(...),
                       run_id: str = Query("990de971-4d15-40eb-b635-d599ccdc169a", description="Run Id")):
    return asyncio.run(get_dag_status_from_id(env, dag, run_id))


@airflow_router.get("/dags/query")
def get_dag_run_status(env: Literal[*env_list] = Query(...),
                       dag: Literal[*dag_list] = Query(...),
                       date_string: str = Query("2025-03-11T11:13:28.992416+00:00"),
                       run_state: str = Query(enum=["success", "failed"], description="Run Status")):
    return asyncio.run(get_query_response(env, dag, date_string, run_state))


@airflow_router.get("/dags/logs")
def get_dag_logs(env: Literal[*env_list] = Query(...),
                 dag: Literal[*dag_list] = Query(...),
                 run_id: str = Query("dc7399e8-c995-4cd1-b9f3-b6ada8836d54", description="Run Id"),
                 try_number: int = 1):
    return asyncio.run(get_dag_logs_from_run_id(env, dag, run_id, try_number))


@airflow_router.post("/dags/runs")
def send_batch_jobs_to_airflow(env: Literal[*env_list] = Query(...),
                               dag: str = Query(None, description="DAG Name",
                                                enum=keyvault["dags-ltops"]),
                               batch_size: int = Query(1, description="Batch Size"),
                               count: int = Query(1, description="Total Jobs to Run")):
    return asyncio.run(async_trigger_dag(env, dag, batch_size, count))
