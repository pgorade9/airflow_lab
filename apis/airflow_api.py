import asyncio

from fastapi import APIRouter, Query, Path

from service.airflow_service import get_dag_status_from_id, async_trigger_dag

airflow_router = APIRouter(prefix="/airflow2/api/v1",
                           tags=["Airflow APIs"], )


@airflow_router.get("/dags/run_status")
def get_dag_run_status(dag_id: str = Query(None, description="DAG name",
                                           enum=["csv_parser_wf_status_gsm", "wellbore_ingestion_wf_gsm",
                                                 "doc_ingestor_azure_ocr_wf",
                                                 "shapefile_ingestor_wf_status_gsm"]),
                       run_id: str = Query("990de971-4d15-40eb-b635-d599ccdc169a", description="Number of items to "
                                                                                               "process")):
    return asyncio.run(get_dag_status_from_id(dag_id, run_id))


@airflow_router.post("/dags/runs")
def send_batch_jobs_to_airflow(env: str = Query(None, description="Environment",
                                                enum=["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops",
                                                      "prod-canary-ltops",
                                                      "prod-aws-ltops",
                                                      "prod-qanoc-ltops"]),
                               dag: str = Query(None, description="DAG Name",
                                                enum=["csv_parser_wf_status_gsm", "wellbore_ingestion_wf_gsm",
                                                      "doc_ingestor_azure_ocr_wf",
                                                      "shapefile_ingestor_wf_status_gsm"]),
                               batch_size: int = Query(1, description="Batch Size"),
                               count: int = Query(1, description="Total Jobs to Run")):
    return asyncio.run(async_trigger_dag(env, dag, batch_size, count))
