from celery import Celery
import redis
from PyPDF2 import PdfReader
import io
import time
from datetime import datetime, timezone
from celery.backends.redis import RedisBackend

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


class SetRedisBackend(RedisBackend):
    def __init__(self, app, url=None, **kwargs):
        super().__init__(app, url, **kwargs)

    def __store_result(
        self, task_id, result, status, traceback=None, request=None, **kwargs
    ):
        mapping = {
            "status": status,
            "taskId": kwargs["taskId"],
            **(result if isinstance(result, dict) else {"result": result}),
        }

        # json.dumps(result) will fail if result contains raw datetime objects — in your case, you’re using time.time() floats,
        # also doing json dumps will convert python dict or object to object string / single string like object. as redis doesnt supports object storage directly in hashes data it supports string only
        self.redis_client.hset(f"TaskId-{task_id}", mapping=mapping)


app = Celery(
    "task",
    broker="amqp://guest:guest@localhost:5672//",
    backend="tasks.SetRedisBackend",
)
app.conf.result_backend = "redis://localhost:6379/0"


@app.task()
def process_pdf(file, taskId):
    queued_at = redis_client.hget(taskId, "Queued At")

    if not queued_at:
        queued_at = time.time()
    started_at = time.time()  # Start Time

    wait_time = started_at - queued_at  # Wait Time

    process_start_time = time.time()  # Process  Start Time

    total_pages = len(PdfReader(io.BytesIO(file)).pages)

    process_end_time = time.time()  # Process End Time
    processing_time = process_end_time - process_start_time
    return {
        "Queued At": datetime.fromtimestamp(queued_at, tz=timezone.utc).isoformat(),
        "Started At": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
        "Wait Time": f"{wait_time:.3f} Seconds",
        "Processing Start Time": datetime.fromtimestamp(
            process_start_time, tz=timezone.utc
        ).isoformat(),
        "Processing End Time": datetime.fromtimestamp(
            process_end_time, tz=timezone.utc
        ).isoformat(),
        "Total Pages": total_pages,
        "Total Processing Time In Seconds": f"{processing_time:.3f} Seconds",
        "taskId": taskId,
    }
