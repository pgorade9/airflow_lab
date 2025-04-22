import asyncio
import uuid
from datetime import datetime

import aiohttp

from configuration import keyvault


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