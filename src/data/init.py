from dotenv import load_dotenv
import os
from sqlmodel import create_engine, SQLModel, Session, text

# Load environment variables from the .env file (if present)
load_dotenv()
# Access environment variables as if they came from the actual environment
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL, echo=False)

def init_db():
    SQLModel.metadata.create_all(engine)

def get_session():
    session = Session(engine)
    return session