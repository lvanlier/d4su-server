##
#   This init2.py is used for Celery tasks to connect to the database.
#
#   It uses psycopg2 instead of psycopg (psycopg3) to connect to the database and create a session object.
#   With psycopg (psycopg3), I get 'Cursor' object has no attribute 'copy_expert' 
#   error when trying to bulk insert data into the database.
#
##

from dotenv import load_dotenv
import os
from sqlmodel import create_engine, Session

# Load environment variables from the .env file (if present)
load_dotenv()
# Access environment variables as if they came from the actual environment
DATABASE_URL2 = os.getenv('DATABASE_URL2')

engine = create_engine(DATABASE_URL2, echo=False)

def get_session():
    return Session(engine)




