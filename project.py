# uvicorn project:app --host 0.0.0.0 --port 9002 --reload
from fastapi import FastAPI, File, UploadFile
import os
from typing import List
from tasks import process_pdf
import uuid
from celery import group
import redis

UPLOADS_FOLDER = "Uploads"
os.makedirs(UPLOADS_FOLDER, exist_ok=True)
app = FastAPI()

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


@app.post("/upload")
async def upload_file(files: List[UploadFile] = File(...)):
    file_contents = []
    for file in files:
        if file.filename.lower().endswith(".pdf"):
            content = await file.read()
            file_contents.append(content)
    job = group(process_pdf.s(content, str(uuid.uuid4())) for content in file_contents)

    job.apply_async(queue="file-processing")
    return {"Status": "Files queued for processing"}
