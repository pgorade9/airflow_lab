from pydantic import BaseModel


class FlowControllerPayload(BaseModel):
    body: dict
    url: str


class DagConf(BaseModel):
    run_id: str
    correlation_id: str
    execution_context: dict
    user_email_id: str
    workflow_name: str
