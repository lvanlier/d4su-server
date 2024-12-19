from fastapi import FastAPI
from web import admin, common, transform, ws, ws_mock_client, taskresult 
from data import init

app = FastAPI()

@app.on_event("startup")
def on_startup():
    init.init_db()

app.include_router(admin.router)
app.include_router(common.router)
app.include_router(transform.router)
app.include_router(ws.router)
app.include_router(ws_mock_client.router)
app.include_router(taskresult.router)

@app.get('/')
def welcome():
    return {"message" : "Root of d4su-Server"}
