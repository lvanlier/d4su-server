from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import pystache
import os
import time
import json

# Load environment variables from the .env file (if present)
from dotenv import load_dotenv
import os
load_dotenv()

baseFileURL = os.getenv("BASE_FILE_URL")
thatOpenURL = os.getenv("THATOPEN_URL")
fastAPIURL = os.getenv("FASTAPI_URL")


router = APIRouter(prefix = "/pages", tags=["Pages"])

cwd = os.path.dirname(os.path.realpath(__file__))

renderer = pystache.Renderer()
common_css_Path = os.path.join(cwd, "templates", "common_css.mustache")
common_js_Path = os.path.join(cwd, "templates", "common_js.mustache")
with open(common_css_Path, "r") as file:
    common_css = file.read()
with open(common_js_Path, "r") as file:
    common_js = file.read()
renderer.partials = {'common_css': common_css, 'common_js': common_js}

@router.get("/", response_class=HTMLResponse)
async def menu():
    # return HTMLResponse(html)
    indexaccordion_path = os.path.join(cwd, "templates", "indexaccordion.mustache")
    with open(indexaccordion_path, "r") as file:
        indexaccordion = file.read()     
    indexjson_path = os.path.join(cwd, "data", "index.json")
    with open(indexjson_path, "r") as file:
        indexjson = json.loads(file.read())
    return HTMLResponse(renderer.render(indexaccordion, indexjson))


@router.get("/ws-client-mockup", response_class=HTMLResponse)
async def ws_client_mockup():
    # return HTMLResponse(html)
    result = {
        "client_id": int(time.time() * 1000)
    }
    ws_client_mockup_path = os.path.join(cwd, "templates", "ws-client-mockup.mustache")
    with open(ws_client_mockup_path, "r") as file:
        ws_client_mockup = file.read()
    
    return HTMLResponse(renderer.render(ws_client_mockup, result))

@router.get("/thatopen-viewer", response_class=HTMLResponse)
async def thatopen_viewer():
    # return HTMLResponse(html)
    thatopenviewer_path = os.path.join(cwd, "templates", "thatopenviewer.mustache")
    with open(thatopenviewer_path, "r") as file:
        thatopenviewer = file.read()     
    mdata = {
        "fastAPIURL" : fastAPIURL,
        "baseFileURL" : baseFileURL,
        "thatOpenURL" : thatOpenURL
    }
    return HTMLResponse(renderer.render(thatopenviewer, mdata))
