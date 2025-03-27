import uvicorn
from fastapi import FastAPI

from apis.airflow_api import airflow_router
from apis.service_bus_api import fc_router

app = FastAPI()


@app.get("/")
def index():
    return {"msg": "Hello World"}


app.include_router(airflow_router)
app.include_router(fc_router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8200)
