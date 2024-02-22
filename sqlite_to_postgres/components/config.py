import os
from dotenv import load_dotenv

load_dotenv()

db_path = os.environ.get("DB_NAME_SQL")


dsn = {
    "dbname": os.environ.get("DB_NAME_POSTGR"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
    "options": "-c search_path=content",
}
