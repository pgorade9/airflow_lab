from pydantic import BaseModel


class flow_controller_payload(BaseModel):
    body: dict
    url: str


class dag_conf(BaseModel):
    run_id: str
    correlation_id: str
    execution_context: dict
    user_email_id: str
    workflow_name: str
