
from sqlmodel import create_engine
from app import settings

connection_string: str = str(settings.DATABASE_URL).replace("postgresql", "postgresql+psycopg")
engine = create_engine(connection_string, pool_recycle=300, pool_size=10, echo=True)

