import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

IMDB_CALENDAR_URL = "https://www.imdb.com/calendar"
BASE_URL = "https://www.imdb.com"

INSTANCE_NAME = os.getenv("CRAWLER_INSTANCE", "crawler_1") 