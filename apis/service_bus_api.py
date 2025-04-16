import asyncio
from typing import Literal

from azure.servicebus.management import SubscriptionRuntimeProperties
from fastapi import Query

from service.message_service import get_service_bus_topic_info, async_batch_message, \
    receive_and_complete_active_messages, receive_and_delete_dead_letter_queue_messages, \
    receive_and_delete_active_messages
from fastapi import routing
from configuration import keyvault

fc_router = routing.APIRouter(prefix="/service_bus",
                              tags=["Service Bus Messaging APIs (data-partition-dependant)"], )

env_list = [key for key in keyvault.keys() if
            isinstance(keyvault[key], dict) and keyvault[key].get("data_partition_id") is not None]

data_partition_list = set()
for key in keyvault.keys():
    if isinstance(keyvault[key], dict) and keyvault[key].get("data_partition_id") not in [None,""]:
        data_partition_list.update(keyvault.get(key).get("data_partitions"))

@fc_router.get("/topic_info")
def get_topic_info(env: Literal[*env_list] = Query(...),
                   data_partition_id: Literal[*data_partition_list] = Query(...)):
    subscription_info = asyncio.run(get_service_bus_topic_info(env, data_partition_id))
    if isinstance(subscription_info, SubscriptionRuntimeProperties):
        return {
            "data_partition_id": f"{data_partition_id}",
            "Active messages": f"{subscription_info.active_message_count}",
            "Dead-letter messages": f"{subscription_info.dead_letter_message_count}",
            "Total messages": f"{subscription_info.total_message_count}"
        }
    else:
        return subscription_info


@fc_router.post("/send_bulk_messages")
def send_bulk_messages(env: Literal[*env_list] = Query(...),
                       data_partition_id: Literal[*data_partition_list] = Query(...),
                       dag: str = Query(None, description="DAG Name",
                                        enum=keyvault["dags-ltops"]),
                       batch_size: int = Query(1, description="Batch Size"),
                       count: int = Query(1, description="Total Jobs to Run")):
    return asyncio.run(async_batch_message(env, data_partition_id, dag, batch_size, count))


@fc_router.get("/complete_active_messages")
def complete_active_messages(env: Literal[*env_list] = Query(...),
                             data_partition_id: Literal[*data_partition_list] = Query(...)):
    return asyncio.run(receive_and_complete_active_messages(env, data_partition_id))


@fc_router.get("/delete_dead_letter_queue_messages")
def delete_dead_letter_queue_messages(env: Literal[*env_list] = Query(...),
                                      data_partition_id: Literal[*data_partition_list] = Query(...)):
    return asyncio.run(receive_and_delete_dead_letter_queue_messages(env, data_partition_id))


@fc_router.get("/delete_active_messages")
def delete_active_messages(env: Literal[*env_list] = Query(...),
                           data_partition_id: Literal[*data_partition_list] = Query(...)):
    return asyncio.run(receive_and_delete_active_messages(env, data_partition_id))
