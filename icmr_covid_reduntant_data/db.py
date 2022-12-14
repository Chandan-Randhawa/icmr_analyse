from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from decouple import config

pg_username = config("PG_USER")
pg_password = config("PG_PASSWORD")
pg_host = config("PG_HOST")
pg_database = config("PG_DB")

SQLALCHEMY_DATABASE_URL = f"postgresql://{pg_username}:{pg_password}@{pg_host}/{pg_database}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL)#, connect_args={"check_same_thread": False}
#)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()

Base = declarative_base()

