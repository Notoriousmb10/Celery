from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import os
from tasks import process_pdf
import uuid

UPLOADS_FOLDER = "Uploads"
os.makedirs(UPLOADS_FOLDER, exist_ok=True)

app = FastAPI()


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Basic validation for PDF extension
    if not file.filename.lower().endswith(".pdf"):
        return JSONResponse(
            content={"error": "Only Pdf files are allowed."}, status_code=400
        )
    taskId = str(uuid.uuid4())
    content = await file.read()
    print("file read")
    process_pdf.apply_async(args=[content, taskId], queue="file-processing")

    return {"message": "File queued for processing", "fileName:": taskId}
