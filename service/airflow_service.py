import asyncio
import time
from pathlib import Path

import aiohttp
import pandas as pd
from fastapi.responses import FileResponse

from configuration import keyvault
from utils.airflow_utils import set_kube_context, port_forward_airflow_web, stop_port_forward, validate_datetime, \
    generateAirflowDagMessage, trigger_dag


async def get_airflow_ui(env: str):
    await set_kube_context(keyvault[env]["cluster"])
    await port_forward_airflow_web(env, keyvault[env]["namespace"])
    msg = f"Connected to Airflow on {env}. Please click here to explore Airflow http://localhost:8100/airflow2/home"
    return {'msg': msg}


async def get_dag_status_from_id(env, dag, dag_run_id):
    await set_kube_context(keyvault[env]["cluster"])
    port_forward_process = await port_forward_airflow_web(env, keyvault[env]["namespace"])
    async with aiohttp.ClientSession() as session:
        url = f"{keyvault[env]['airflow_url']}/api/v1/dags/{dag}/dagRuns/{dag_run_id}"
        username = keyvault[env]["airflow_username"]
        password = keyvault[env]["airflow_password"]
        auth = aiohttp.BasicAuth(username, password)
        headers = {"accept": "application/json"}
        async with session.get(url, auth=auth, headers=headers) as response:
            response_json = await response.json()
            await stop_port_forward(port_forward_process)
            if response.status == 200:
                return response_json
            else:
                return {"status": f"Run id {dag_run_id} Not Found"}


async def get_query_response(env, dag, date_string, run_state):
    if not validate_datetime(date_string):
        return {"msg": "Please use correct date time format as YYYY-MM-DDTHH:MM:SS.ssssss±HH:MM"}
    await set_kube_context(keyvault[env]["cluster"])
    port_forward_process = await port_forward_airflow_web(env, keyvault[env]["namespace"])
    async with aiohttp.ClientSession() as session:
        url = f"{keyvault[env]['airflow_url']}/api/v1/dags/{dag}/dagRuns"
        username = keyvault[env]["airflow_username"]
        password = keyvault[env]["airflow_password"]
        auth = aiohttp.BasicAuth(username, password)
        headers = {"accept": "application/json"}
        # valid_params : execution_date_gte, state, execution_date_lte, order_by
        params = {
            "execution_date_gte": date_string,
            "state": run_state
        }
        async with session.get(url, auth=auth, headers=headers, params=params) as response:
            print(f"{url=}{params=}")
            response_json = await response.json()
            await stop_port_forward(port_forward_process)
            if response.status == 200:
                return response_json


async def get_dag_logs_from_run_id(env, dag, dag_run_id, try_number):
    await set_kube_context(keyvault[env]["cluster"])
    port_forward_process = await port_forward_airflow_web(env, keyvault[env]["namespace"])
    async with aiohttp.ClientSession() as session:
        url = f"{keyvault[env]['airflow_url']}/api/v1/dags/{dag}/dagRuns/{dag_run_id}/taskInstances/{keyvault['task-id'][dag]}/logs/{try_number}"
        username = keyvault[env]["airflow_username"]
        password = keyvault[env]["airflow_password"]

        auth = aiohttp.BasicAuth(username, password)

        async with session.get(url, auth=auth) as response:
            response_text = await response.text()
            await stop_port_forward(port_forward_process)
            if response.status == 200:
                FILENAME = f"{dag_run_id}.txt"
                OUTPUT = Path("output")
                PATH = Path(f"{OUTPUT}/{FILENAME}")
                print(f"{PATH=}")
                with open(PATH, "w+") as fp:
                    for line in response_text:
                        fp.write(line)
                return FileResponse(path=PATH, filename=FILENAME, media_type="application/octet-stream")
                # return response_text
            else:
                return {"status": f"Run id {dag_run_id} Not Found"}


async def async_trigger_dag(env, dag, batch_size, count):
    await set_kube_context(keyvault[env]["cluster"])
    dataframe = pd.DataFrame(columns=['run_Id'])

    batch_counter = 0
    number_of_batches = int(count / batch_size)
    print(f"{number_of_batches=}")

    for _ in range(number_of_batches):
        tasks = [trigger_dag(env, dag, dataframe) for _ in range(batch_size)]
        await asyncio.gather(*tasks)
        batch_counter += 1
        print(f"Completed {batch_counter} batch of {batch_size} messages")
    return {"msg": f"Sent Batch of {count} jobs to Airflow successfully !!"}


if __name__ == "__main__":
    envs_ltops = ["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops", "prod-canary-ltops", "prod-aws-ltops",
                  "prod-qanoc-ltops"]
    dags_ltops = ["csv_parser_wf_status_gsm", "wellbore_ingestion_wf_gsm", "doc_ingestor_azure_ocr_wf",
                  "shapefile_ingestor_wf_status_gsm"]

    TIMEOUT = 30
    start_time = int(time.time())
    df = pd.DataFrame(columns=['run_Id'])

    asyncio.run(async_trigger_dag(env=envs_ltops[4],
                                  dag=dags_ltops[-1],
                                  batch_size=1,
                                  count=1))
