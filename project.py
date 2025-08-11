from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import shutil
import os
from tasks import process_pdf


UPLOADS_FOLDER = "Uploads"
os.makedirs(UPLOADS_FOLDER, exist_ok=True)

app = FastAPI


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.lower().endswith("pdf"):
        return JSONResponse(
            content={"error": "Only Pdf files are allowed."}, status_code=400
        )
    file_path = os.path.join(
        UPLOADS_FOLDER, file.filename
    )  # join folder name and filename eg /uploads/yash.pdf
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    task = process_pdf.delay(file_path)

    return {"message": "File queued for processing", "taskId": task.id}
