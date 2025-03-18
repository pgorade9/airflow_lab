import asyncio
import json
import time
import uuid

import pandas as pd
from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage, ServiceBusReceiveMode, ServiceBusSubQueue
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus.aio.management import ServiceBusAdministrationClient

from configuration import keyvault

batch_counter = 0
TIMEOUT = 30


def generateFlowControllerMessage(env, data_partition_id, dag, df):
    conf = {}
    unique_id = str(uuid.uuid4())
    # Please check dag conf keys for each dag in its repo for example execution_context or executionContext
    conf["run_id"] = f"{unique_id}"
    conf["correlation_id"] = f"{unique_id}"
    conf["execution_context"] = {}
    conf["execution_context"]["dataPartitionId"] = data_partition_id
    conf["execution_context"]["id"] = keyvault[env]["file_id"][dag]

    conf["user_email_id"] = keyvault[env]["client_id"]
    conf["workflow_name"] = dag
    message = {
        "body": {
            "dag_run_id": unique_id,
            "conf": conf
        },
        "url": f"http://airflow-app-web:8080/airflow2/api/v1/dags/{dag}/dagRuns"
    }
    message_body = json.dumps(message)
    # print(f"{message_body=}")
    entry = {'run_Id': unique_id}
    df.loc[len(df)] = entry
    return message_body


async def send_batch_message(env, data_partition_id, dag, batch_size, timeout, dataframe):
    global batch_counter

    try:

        async with ServiceBusClient.from_connection_string(
                conn_str=keyvault[env]["CONNECTION_STRING"][data_partition_id],
                logging_enable=False) as service_bus_client:
            async with service_bus_client.get_topic_sender(topic_name=keyvault["TOPIC_NAME"],
                                                          socket_timeout=timeout) as sender:
                batch = await sender.create_message_batch()
                for _ in range(batch_size):
                    try:
                        batch.add_message(
                            ServiceBusMessage(generateFlowControllerMessage(env, data_partition_id, dag, dataframe)))
                    except ValueError:
                        break
                await sender.send_messages(batch)
                batch_counter += 1
                print(f"Completed sending {batch_counter} batch of size {batch_size}")
    except KeyError as e:
        raise e


async def async_batch_message(env, data_partition_id, dag, batch_size, count):
    timeout = 30
    dataframe = pd.DataFrame(columns=['run_Id'])

    number_of_batches = int(count / batch_size)
    print(f"{number_of_batches=}")
    task_size = 10 if number_of_batches % 10 == 0 else 1
    print("**********************************")
    print(f"{task_size=}")
    print("**********************************")
    task_group_size = int(number_of_batches / task_size)

    try:
        for _ in range(task_group_size):
            tasks = [send_batch_message(env, data_partition_id, dag, batch_size, timeout, dataframe) for _ in
                     range(task_size)]
            await asyncio.gather(*tasks)

        return {
            "msg": f"Sent Batch of {count} jobs to flow-controller topic on {data_partition_id} partition successfully"}
    except KeyError as e:
        return {"msg": f"Not found Connection String for {data_partition_id=}"}


async def process_message(receiver, msg: ServiceBusReceivedMessage, counter):
    try:
        # print(f"Received message: {msg.message_id}, Body: {msg.body}")
        await receiver.complete_message(msg)
        counter += 1
        print(f"{counter} Message completed successfully")
    except Exception as e:
        print(f"Error processing message: {e}")


async def receive_and_complete_active_messages(env, data_partition_id):
    try:
        async with ServiceBusClient.from_connection_string(
                conn_str=keyvault[env]["CONNECTION_STRING"][data_partition_id], logging_enable=False
        ) as service_bus_client:
            async with service_bus_client.get_subscription_receiver(
                    topic_name=keyvault["TOPIC_NAME"],
                    subscription_name=keyvault["SUBSCRIPTION_NAME"],
                    receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
                    max_wait_time=TIMEOUT
            ) as receiver:
                print(f"Starting to receive Active messages from {keyvault["SUBSCRIPTION_NAME"]}")
                async for msg in receiver:  # Async generator
                    await process_message(receiver, msg, counter=0)
    except Exception as e:
        print(f"Error initializing ServiceBusClient: {e}")
    return {"msg": f"Done Receiving and completing Active messages from {data_partition_id=}"}


async def receive_and_delete_dead_letter_queue_messages(env, data_partition_id):
    try:
        async with ServiceBusClient.from_connection_string(
                conn_str=keyvault[env]["CONNECTION_STRING"][data_partition_id], logging_enable=False
        ) as service_bus_client:
            async with service_bus_client.get_subscription_receiver(
                    topic_name=keyvault["TOPIC_NAME"],
                    subscription_name=keyvault["SUBSCRIPTION_NAME"],
                    sub_queue=ServiceBusSubQueue.DEAD_LETTER,
                    receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
                    max_wait_time=TIMEOUT
            ) as receiver:
                while True:
                    messages = await receiver.receive_messages(max_message_count=100, max_wait_time=5)
                    print(f"Deleted {len(messages)} messages")
                    if not messages:
                        break  # Stop when no more messages
        return {"msg": "Done Receiving and completing Dead Letter Queue messages"}
    except KeyError as e:
        return {"msg": f"Not found Connection String for {data_partition_id=}"}


async def get_service_bus_topic_info(env, data_partition_id):
    try:
        # Create a client to manage Service Bus
        async with ServiceBusAdministrationClient.from_connection_string(
                keyvault[env]["CONNECTION_STRING"][data_partition_id]) as service_bus_admin_client:
            # Get topic subscription details
            subscription_info = await service_bus_admin_client.get_subscription_runtime_properties(keyvault["TOPIC_NAME"],
                                                                                       keyvault["SUBSCRIPTION_NAME"])
            return subscription_info
    except KeyError as e:
        return {"msg": f"Not found Connection String for {data_partition_id=}"}


async def receive_and_delete_active_messages(env, data_partition_id):
    try:
        async with ServiceBusClient.from_connection_string(
                keyvault[env]["CONNECTION_STRING"][data_partition_id]) as service_bus_client:
            async with service_bus_client.get_subscription_receiver(
                    topic_name=keyvault["TOPIC_NAME"],
                    subscription_name=keyvault["SUBSCRIPTION_NAME"],
                    receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE  # Messages are deleted immediately
            ) as receiver:
                while True:
                    messages = await receiver.receive_messages(max_message_count=100, max_wait_time=5)
                    print(f"Deleted {len(messages)} messages")
                    if not messages:
                        break  # Stop when no more messages
        return {"msg": "All Active Messages Deleted from {data_partition_id=}!"}
    except KeyError as e:
        return {"msg": f"Not found Connection String for {data_partition_id=}"}


if __name__ == "__main__":
    envs_ltops = ["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops", "prod-canary-ltops", "prod-aws-ltops",
                  "prod-qanoc-ltops"]
    dags_ltops = ["csv_parser_wf_status_gsm", "wellbore_ingestion_wf_gsm", "doc_ingestor_azure_ocr_wf",
                  "shapefile_ingestor_wf_status_gsm"]

    TIMEOUT = 30
    # start_time = int(time.time())
    # df = pd.DataFrame(columns=['run_Id'])

    # Trigger Messages Endpoint
    # asyncio.run(async_batch_message(env=envs_ltops[4],
    #                                 dag=dags_ltops[-1],
    #                                 batch_size=1,
    #                                 count=1))
    # df.to_excel("runIds.xlsx")

    # Get Topic Info
    # topic_info = asyncio.run(get_service_bus_topic_info(env=envs_ltops[4]))
    # print(f"Active Messages : {topic_info.active_message_count}")

    # Receive and complete messages
    # asyncio.run(receive_and_complete_messages(env=envs_ltops[4]))

    #  Receive and delete messages
    # asyncio.run(receive_and_delete_messages(env=envs_ltops[4]))
