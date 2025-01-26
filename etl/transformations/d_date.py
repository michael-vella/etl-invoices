import pandas as pd
from etl.transformations.base import ETLBase
from etl.logger import get_logger
from sqlalchemy.orm import Session, sessionmaker
from etl.db.d_date import DateDimension


class ETLDateDimension(ETLBase):
    """ETL logic used to create date dimension"""
    def __init__(self, table_name: str, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.table_name: str = table_name
        self.db_session: sessionmaker[Session] = session

    def _create_date_dim(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Create date dimension"""
        date_range = pd.date_range(start=start_date, end=end_date)

        dim_date = pd.DataFrame({
            "date_key": date_range.strftime("%Y%m%d").astype(int),  # YYYYMMDD format as integer
            "date": date_range,
            "year": date_range.year,
            "month": date_range.strftime("%B"),  # full month name
            "day_of_month": date_range.day,
            "day_of_week": date_range.strftime("%A"),  # full day name
        })

        return dim_date


    def run_etl(self) -> pd.DataFrame:
        """Concrete implementation of run_etl abstract method."""
        self.logger.info("Truncating table for full-load")
        self.truncate_table(table_name=self.table_name, session=self.db_session)

        self.logger.info("Creating date dimension in pandas")
        df: pd.DataFrame = self._create_date_dim(start_date="2009-01-01", end_date="2010-12-31")
        df = self.create_insert_txstamp(df=df)

        self.logger.info("Inserting dataframe into table")
        records = df.to_dict(orient="records")
        self.db_session.bulk_insert_mappings(DateDimension, records)

        self.logger.info("Date dimension ETL step successful")

        return df
