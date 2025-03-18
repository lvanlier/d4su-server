from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import pystache
import os
import time


router = APIRouter(prefix = "/wsinit")
cwd = os.path.dirname(os.path.realpath(__file__))

@router.get("/")
async def get():
    # return HTMLResponse(html)
    templatePath = os.path.join(cwd, "templates", "ws-client-mockup.html")
    result = {
        "client_id": int(time.time() * 1000)
    }
    with open(templatePath, "r") as file:
        return HTMLResponse(pystache.render(file.read(), result))
