import asyncio

from fastapi import APIRouter, Query, Path
from fastapi.responses import FileResponse

from service.airflow_service import get_dag_status_from_id, async_trigger_dag, get_dag_logs_from_run_id
from configuration import keyvault

airflow_router = APIRouter(prefix="/airflow2/api/v1",
                           tags=["Airflow APIs"], )


@airflow_router.get("/dags/run_status")
def get_dag_run_status(env: str = Query(None, description="Environment",
                                                enum=keyvault["envs-ltops"]),
                       dag: str = Query(None, description="DAG Name",
                                        enum=keyvault["dags-ltops"]),
                       run_id: str = Query("990de971-4d15-40eb-b635-d599ccdc169a", description="Run Id")):
    return asyncio.run(get_dag_status_from_id(env, dag, run_id))


@airflow_router.get("/dags/logs")
def get_dag_logs(env: str = Query("evd-ltops", description="Environment",
                                                enum=keyvault["envs-ltops"]),
                       dag: str = Query("shapefile_ingestor_wf_status_gsm", description="DAG Name",
                                        enum=keyvault["dags-ltops"]),
                       run_id: str = Query("dc7399e8-c995-4cd1-b9f3-b6ada8836d54", description="Run Id")):
    return asyncio.run(get_dag_logs_from_run_id(env, dag, run_id))


@airflow_router.post("/dags/runs")
def send_batch_jobs_to_airflow(env: str = Query(None, description="Environment",
                                                enum=keyvault["envs-ltops"]),
                               dag: str = Query(None, description="DAG Name",
                                                enum=keyvault["dags-ltops"]),
                               batch_size: int = Query(1, description="Batch Size"),
                               count: int = Query(1, description="Total Jobs to Run")):
    return asyncio.run(async_trigger_dag(env, dag, batch_size, count))
