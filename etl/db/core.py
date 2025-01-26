import pyodbc
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, Engine
from etl.logger import get_logger


class Base(DeclarativeBase):
    """Base class used by SQLAlchemy."""
    pass


class DBContext:
    """Class used to connect and create tables on Microsoft SQL Server."""
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    def create_db(self, server: str, username: str, password: str, db_name: str) -> None:
        """Create database if it doesn't exist on the server."""
        try:
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password};"
            )
            self.logger.info("Successful connection to Microsoft SQL Server.")

            conn.autocommit = True
            conn.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}') BEGIN CREATE DATABASE {db_name}; END;")

            self.logger.info(f"SQL query database creation for '{db_name}' database ran successfully.")
        except Exception as e:
            self.logger.critical(f"Error connecting to the database: {e}")

    def get_engine(self, server: str, username: str, password: str, db_name: str) -> Engine:
        """Creates and returns a SQLAlchemy engine."""
        connection_string: str = f"mssql+pyodbc://{username}:{password}@{server}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
        self.logger.info(f"Creating SQLAlchemy engine to connect with Microsoft SQL Server '{db_name}' database.")
        engine: Engine = create_engine(connection_string, echo=True)
        self.logger.info("SQLAlchemy engine created successfully.")
        return engine

    def create_tables(self, engine: Engine) -> None:
        """Creates tables based on the defined models."""
        try:
            Base.metadata.create_all(engine)
            self.logger.info("Tables created successfully.")
        except SQLAlchemyError as e:
            self.logger.critical(f"Error creating tables: {e}")
