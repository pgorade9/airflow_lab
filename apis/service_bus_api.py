import asyncio

from fastapi import Query

from service.message_service import get_service_bus_topic_info, async_batch_message, \
    receive_and_complete_messages, receive_and_delete_messages
from fastapi import routing

fc_router = routing.APIRouter(prefix="/service_bus",
                              tags=["Service Bus Messaging APIs"], )


@fc_router.get("/topic_info")
def get_topic_info(env: str = Query(None, description="Environment",
                                    enum=["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops", "prod-canary-ltops",
                                          "prod-aws-ltops",
                                          "prod-qanoc-ltops"])):
    # return {"active_messages": "40"}
    subscription_info = asyncio.run(get_service_bus_topic_info(env))
    return {
        "Active messages": f"{subscription_info.active_message_count}",
        "Dead-letter messages": f"{subscription_info.dead_letter_message_count}",
        "Total messages": f"{subscription_info.total_message_count}"
    }


@fc_router.post("/send_bulk_messages")
def send_bulk_messages(env: str = Query(None, description="Environment",
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
    return asyncio.run(async_batch_message(env, dag, batch_size, count))


@fc_router.get("/complete_messages")
def complete_messages(env: str = Query(None, description="Environment",
                                       enum=["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops",
                                             "prod-canary-ltops",
                                             "prod-aws-ltops",
                                             "prod-qanoc-ltops"])):
    return asyncio.run(receive_and_complete_messages(env))


@fc_router.get("/delete_messages")
def delete_messages(env: str = Query(None, description="Environment",
                                     enum=["evd-ltops", "evt-ltops", "adme-outerloop", "mde-ltops",
                                           "prod-canary-ltops",
                                           "prod-aws-ltops",
                                           "prod-qanoc-ltops"])):
    return asyncio.run(receive_and_delete_messages(env))
