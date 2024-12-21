import logging
from celery import Celery

from dotenv import load_dotenv
import os
import sys

# Load environment variables from the .env file (if present)
load_dotenv()
# Access environment variables as if they came from the actual environment
CELERY_BROKER = os.getenv('CELERY_BROKER')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND')

app = Celery(
    'tasks',
    broker=CELERY_BROKER,
    backend=CELERY_RESULT_BACKEND,
    include=['long_bg_tasks.tasks']
)

app.conf.update(
    result_expires=3600,
    timezone = 'Europe/Brussels',
    broker_connection_retry_on_startup=True, 
)

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

if __name__ == '__main__':
    app.start()
