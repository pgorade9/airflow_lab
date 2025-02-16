import asyncio
import time
import uuid
import pandas as pd
import aiohttp

from configuration import keyvault


async def get_dag_status_from_id(dag, dag_run_id):
    async with aiohttp.ClientSession() as session:
        url = f"{keyvault["airflow_url"]}/{dag}/dagRuns/{dag_run_id}"
        username = keyvault["airflow_username"]
        password = keyvault["airflow_password"]
        auth = aiohttp.BasicAuth(username, password)
        # print(type(payload))
        async with session.get(url, auth=auth) as response:
            response_json = await response.json()
            if response.status == 200:
                return response_json
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

        url = f"{keyvault["airflow_url"]}/{dag}/dagRuns"
        username = keyvault["airflow_username"]
        password = keyvault["airflow_password"]
        auth = aiohttp.BasicAuth(username, password)
        payload = generateAirflowDagMessage(env, dag, dataframe)
        # print(type(payload))
        async with session.post(url, auth=auth, json=payload) as response:
            data = await response.json()
            if response.status == 200:
                # print("✅ DAG Triggered:", data["dag_run_id"])
                entry = {'run_Id': data["dag_run_id"]}
                dataframe.loc[len(dataframe)] = entry
            else:
                print(f"❌ Failed: {response.status}, {data}")


async def async_trigger_dag(env, dag, batch_size, count):
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
