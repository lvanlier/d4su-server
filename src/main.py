from fastapi import FastAPI
from web import admin, common, pages, transform, ws, taskresult, ifc5dev 
from data import init
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import os
load_dotenv()

VIEWER_PATH = os.getenv('VIEWER_PATH')


app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3100",
    "http://localhost:8080"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/ifc5-dev", StaticFiles(directory=VIEWER_PATH), name="ifc5-dev")

@app.on_event("startup")
def on_startup():
    init.init_db()

app.include_router(admin.router)
app.include_router(common.router)
app.include_router(transform.router)
app.include_router(ws.router)
app.include_router(pages.router)
app.include_router(taskresult.router)
app.include_router(ifc5dev.router)



@app.get('/')
def welcome():
    return {"message" : "Root of d4su-Server"}

