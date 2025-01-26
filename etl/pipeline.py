import pandas as pd
from etl.constants import (
    DIM_CUSTOMER_TABLE_NAME,
    DIM_DATE_TABLE_NAME,
    DIM_INVOICE_TABLE_NAME,
    DIM_PRODUCT_TABLE_NAME,
    FACT_TRANSACTION_TABLE_NAME
)
from etl.logger import get_logger
from etl.transformations.d_date import ETLDateDimension
from sqlalchemy.orm import Session, sessionmaker


class ETLPipeline():
    """ETL Pipeline class that will invoke ETL steps required to full-load invoices CSV file."""
    def __init__(self, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.etl_date_dim = ETLDateDimension(table_name=DIM_DATE_TABLE_NAME, session=session)

    def run_pipeline(self, df: pd.DataFrame):
        """Run ETL pipeline to full-load invoices CSV file."""
        self.logger.info("Run date timension etl step")
        date_dim = self.etl_date_dim.run_etl()
        