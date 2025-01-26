import pyodbc
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, Engine


class Base(DeclarativeBase):
    pass


def create_db(server: str, username: str, password: str, db_name: str) -> None:
    """Create database if it doesn't exist on the server."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};"
        )
        conn.autocommit = True
        conn.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}') BEGIN CREATE DATABASE {db_name}; END;")
        print("Connection successful!")
    except Exception as e:
        print("Error connecting to the database:", e)

def get_engine(server: str, username: str, password: str, db_name: str) -> Engine:
    """Creates and returns a SQLAlchemy engine."""
    connection_string: str = f"mssql+pyodbc://{username}:{password}@{server}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
    engine: Engine = create_engine(connection_string, echo=True)
    return engine

def create_tables(engine: Engine) -> None:
    """Creates tables based on the defined models."""
    try:
        Base.metadata.create_all(engine)
        print("Tables created successfully.")
    except SQLAlchemyError as e:
        print(f"Error creating tables: {e}")
