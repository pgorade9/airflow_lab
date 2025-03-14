import asyncio
import time
import uuid
from datetime import datetime

import pandas as pd
import aiohttp
from pathlib import Path

from configuration import keyvault
from fastapi.responses import FileResponse


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


def validate_datetime(date_string):
    try:
        # Define the expected format
        expected_format = "%Y-%m-%dT%H:%M:%S.%f%z"

        # Try parsing the datetime string
        parsed_date = datetime.strptime(date_string, expected_format)

        print(f"Valid datetime: {parsed_date}")
        return True
    except ValueError:
        print("Invalid datetime format!")
        return False


# Test the function
datetime_str = "2025-03-11T11:13:28.992416+00:00"
validate_datetime(datetime_str)


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


def generateAirflowDagMessage(env, dag, dataframe):
    conf = {}
    unique_id = str(uuid.uuid4())
    # Please check dag conf keys for each dag in its repo for example execution_context or executionContext
    conf["run_id"] = f"{unique_id}"
    conf["correlation_id"] = f"{unique_id}"
    conf["execution_context"] = {}
    conf["execution_context"]["dataPartitionId"] = keyvault[env]["data_partition_id"]
    conf["execution_context"]["id"] = keyvault[env]["file_id"][dag]
    conf["user_email_id"] = keyvault[env]["client_id"]
    conf["workflow_name"] = dag

    message = {
        "dag_run_id": unique_id,
        "conf": conf
    }
    # print(f"{json.dumps(message, indent=4)}")
    entry = {'run_Id': unique_id}
    dataframe.loc[len(dataframe)] = entry
    return message


async def trigger_dag(env, dag, dataframe):
    async with aiohttp.ClientSession() as session:

        url = f"{keyvault[env]["airflow_url"]}/api/v1/dags/{dag}/dagRuns"
        username = keyvault[env]["airflow_username"]
        password = keyvault[env]["airflow_password"]
        auth = aiohttp.BasicAuth(username, password)
        payload = generateAirflowDagMessage(env, dag, dataframe)
        # print(type(payload))
        async with session.post(url, auth=auth, json=payload) as response:
            data = await response.json()
            if response.status == 200:
                entry = {'run_Id': data["dag_run_id"]}
                dataframe.loc[len(dataframe)] = entry
            else:
                print(f"Failed: {response.status}, {data}")


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


# This method is only required for airflow api
async def set_kube_context(context: str):
    """Asynchronously sets the Kubernetes context using kubectl."""
    process = await asyncio.create_subprocess_exec(
        "kubectl", "config", "use-context", context,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        print(f"Context changed to: {context}")
        print(stdout.decode().strip())
    else:
        print(f"Failed to change context to: {context}")
        print(stderr.decode().strip())


async def port_forward_airflow_web(env, namespace: str):
    """Runs kubectl port-forward in daemon mode (background)."""
    print(f"Starting port-forwarding for namespace: {namespace}")
    service_name = "svc/airflow-app-web" if namespace != "airflow" else "svc/airflow-web"
    process = await asyncio.create_subprocess_exec(
        "kubectl", "port-forward", service_name, "8100:8080", "-n", namespace,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await validate_port_readiness(env)
    return process


async def validate_port_readiness(env):
    print("Validating port readiness.")
    url = f"{keyvault[env]['airflow_url']}/health"
    username = keyvault[env]["airflow_username"]
    password = keyvault[env]["airflow_password"]
    auth = aiohttp.BasicAuth(username, password)
    headers = {"accept": "application/json"}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, auth=auth, headers=headers) as response:
                    response_json = await response.json()
                    print(f"health response {response_json['scheduler']['status']=}")
                    if response.status == 200:
                        break
                    await asyncio.sleep(1)
            except Exception as e:
                print(f"Connection not ready for {url}.......")


async def stop_port_forward(process):
    """Gracefully stops the port-forwarding process."""
    if not process or process.returncode is not None:
        print("Port-forward process is already stopped.")
        return

    print("\nStopping port-forward...")

    try:
        process.terminate()  # Send SIGTERM
        try:
            await asyncio.wait_for(process.wait(), timeout=5)  # Wait up to 5 seconds
        except asyncio.TimeoutError:
            print("Graceful termination timed out. Killing process...")
            process.kill()  # Force stop
            await process.wait()

        print("Port-forwarding stopped.")
    except Exception as e:
        print(f"Error stopping port-forward: {e}")


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
