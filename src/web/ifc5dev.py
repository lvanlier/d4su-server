from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import RedirectResponse

from data import admin as data 
from service import ifc5dev as service

router = APIRouter(prefix = "/ifc5dev")


@router.post("/uploadfile/", tags=["Internal", "Ifc5dev"])
async def create_upload_file(file: UploadFile):
    print(file.filename)
    print(file.content_type)
    await service.store_uploaded_file(file)
    return {"filename": file.filename}

@router.get("/viewer", response_class=RedirectResponse, tags=["Ifc5dev"])
async def redirect_viewer():
    return "http://localhost:8000/ifc5-dev/viewer-v01/index.html"
