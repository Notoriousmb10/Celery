from celery import Celery
import time
import redis

import fitz

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
app = Celery(
    "task",
    broker="amqp://guest:guest@localhost:5672//",
    backend="redis://localhost:6379/0",
)


@app.task(bind=True)
def process_pdf(self, file_path):
    task_id = self.request.id
    queued_at = redis_client.hget(task_id, "Queued At")
    if not queued_at:
        redis_client.hset(task_id, "Queeud At", time.time())
    else:
        queued_at = int(queued_at)

    started_at = int(time.time())
    redis_client.hset(task_id, "Started At", time.time())

    wait_time = started_at - queued_at
    redis_client.hset(task_id, "Wait Time", wait_time)

    process_start_time = time.time()

    doc = fitz.open(file_path)
    total_pages = len(doc)
    doc.close()

    process_end_time = int(time.time())
    redis_client.hset(task_id, "Processing End Time", process_end_time)
    redis_client.hset(task_id, "Total Pages ", total_pages)
    processing_time = round(process_end_time - process_start_time, 3)
    redis_client.hset(task_id, "Total Processing Time", processing_time)
    return {
        "task_id": task_id,
        "file_path": file_path,
        "wait_time": wait_time,
        "processing_time": processing_time,
        "total_pages": total_pages,
    }
