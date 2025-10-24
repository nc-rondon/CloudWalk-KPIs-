import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

class SessionConnector:
    def __init__(self) -> None:
        load_dotenv()
        self.pg_url = os.getenv(
            "PG_URL",
            "postgresql+psycopg2://metabase:metabase@localhost:5432/analytics"
        )

    def session(self):
        return create_engine(self.pg_url, echo=False)
