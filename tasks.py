import celery from Celery
import time
import redis

redis_client = redis.Redis(host='localhost', port="6379", decode_response=True)
app = Celery(
    'task', broker='amqp://guest:guest@localhost:5672//', backend='redis//localhost:6379/0'
)

@app.task
def process_pdf(file_path):
    redis_client.hset('Qeueud At', int(time.time()))
    