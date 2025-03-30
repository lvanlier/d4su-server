from fastapi import UploadFile

from dotenv import load_dotenv
import os
load_dotenv()

BASE_PATH = os.getenv('BASE_PATH')
UPLOAD_PATH = os.getenv('UPLOAD_PATH')

async def store_uploaded_file(file: UploadFile):
    file2store = await file.read()
    with open(f"{BASE_PATH}{UPLOAD_PATH}/{file.filename}", "wb") as f:
        f.write(file2store)
    return {"filename": file.filename}

