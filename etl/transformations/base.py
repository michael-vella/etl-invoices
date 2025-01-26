import pandas as pd
import datetime
from abc import ABC, abstractmethod
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text

class ETLBase(ABC):
    """ETL base class that will house different etl steps."""
    def create_insert_txstamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Method to create _insert_txstamp column to a DataFrame."""
        df['_insert_txstamp'] = datetime.datetime.now()
        return df

    def truncate_table(self, table_name: str, session: sessionmaker[Session]) -> None:
        """Method to truncate table."""
        session.execute(text(f"TRUNCATE TABLE {table_name};"))

    def create_surrogate_key(self, surrogate_key_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Create a surrogate key inside the DataFrame"""
        df[surrogate_key_name] = range(1, len(df) + 1)
        return df
    
    @abstractmethod
    def run_etl(self) -> None:
        """Abstract method to be initialised in subclasses.
        Subclasses will write specific ETL logic based on their scenario."""