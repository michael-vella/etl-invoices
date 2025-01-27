import pandas as pd
from etl.transformations.base import ETLBase
from etl.logger import get_logger
from sqlalchemy.orm import Session, sessionmaker
from etl.db.d_customer import CustomerDimension


class ETLCustomerDimension(ETLBase):
    """ETL logic used to create customer dimension"""
    def __init__(self, table_name: str, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.table_name: str = table_name
        self.db_session: sessionmaker[Session] = session

    def _select_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select only the required columns for the customer dimension."""
        return df[["customer_id", "country"]]

    def _create_customer_dim(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create customer dimension"""
        df = self._select_required_columns(df)
        distinct_df: pd.DataFrame = df.drop_duplicates()
        distinct_df = self.create_surrogate_key(surrogate_key_name="customer_key", df=distinct_df)
        return distinct_df

    def run_etl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Concrete implementation of run_etl abstract method."""
        self.logger.info("Truncating table for full-load")
        self.truncate_table(table_name=self.table_name, session=self.db_session)

        self.logger.info("Creating customer dimension in pandas")
        df: pd.DataFrame = self._create_customer_dim(df=df)
        df = self.create_insert_txstamp(df=df)

        self.logger.info("Inserting dataframe into table")
        records = df.to_dict(orient="records")
        self.db_session.bulk_insert_mappings(CustomerDimension, records)

        self.logger.info("customer dimension ETL step successful")

        return df
