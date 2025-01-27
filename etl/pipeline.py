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
from etl.transformations.d_invoice import ETLInvoiceDimension
from etl.transformations.d_customer import ETLCustomerDimension
from etl.transformations.d_product import ETLProductDimension
from etl.transformations.f_transaction import ETLTransactionFact
from sqlalchemy.orm import Session, sessionmaker


class ETLPipeline():
    """ETL Pipeline class that will invoke ETL steps required to full-load invoices CSV file."""
    def __init__(self, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.etl_date_dim = ETLDateDimension(table_name=DIM_DATE_TABLE_NAME, session=session)
        self.etl_invoice_dim = ETLInvoiceDimension(table_name=DIM_INVOICE_TABLE_NAME, session=session)
        self.etl_customer_dim = ETLCustomerDimension(table_name=DIM_CUSTOMER_TABLE_NAME, session=session)
        self.etl_product_dim = ETLProductDimension(table_name=DIM_PRODUCT_TABLE_NAME, session=session)
        self.etl_transaction_fact = ETLTransactionFact(table_name=FACT_TRANSACTION_TABLE_NAME, session=session)

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns inside a pandas DataFrame"""
        return df.rename(
            columns={
                "Invoice": "invoice_no",
                "StockCode": "code",
                "Description": "description",
                "Quantity": "quantity",
                "InvoiceDate": "invoice_date",
                "Price": "price",
                "Customer ID": "customer_id",
                "Country": "country",
            }
        )

    def _cast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns inside a pandas DataFrame"""
        df["invoice_no"] = df["invoice_no"].astype(str)
        df["code"] = df["code"].astype(str)
        df["description"] = df["description"].astype(str)
        df["quantity"] = df["quantity"].astype(int)
        df["invoice_date"] = pd.to_datetime(df["invoice_date"]).dt.date
        df["price"] = df["price"].astype(float)

        # convert to int, coerce errors to NULL then default NULL with -1
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors='coerce').fillna(-1).astype(int)

        df["country"] = df["country"].astype(str)

        return df
    
    def _perform_data_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Performing data cleaning. Filter out stock codes if they contain 'TEST'."""
        # 'na = False' does not filter out null values
        return df[~df["code"].str.contains('TEST', na=False)]
    
    def _create_quantity_class_column(self, df: pd.DataFrame):
        """Create new column `quantity_class` inside DataFrame"""
        # if x > 0, positive
        # if x < 0, negative
        # if x = 0, zero
        df['quantity_class'] = df['quantity'].apply(lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'zero'))
        return df
    
    def _create_price_class_column(self, df: pd.DataFrame):
        """Create new column `price_class` inside DataFrame"""
        # if x > 0, positive
        # if x < 0, negative
        # if x = 0, zero
        df['price_class'] = df['price'].apply(lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'zero'))
        return df
    
    def _categorize_invoice(self, row):
        """Logic to create new column 'type'."""
        if row['quantity_class'] == 'negative' and row['price_class'] == 'positive':
            return 'Purchase'
        elif row['quantity_class'] == 'negative' and row['price_class'] == 'zero':
            return 'Free Stock'
        elif row['quantity_class'] == 'positive' and row['price_class'] == 'negative':
            return 'Adjustment'
        elif row['quantity_class'] == 'positive' and row['price_class'] == 'positive':
            return 'Sale'
        elif row['quantity_class'] == 'positive' and row['price_class'] == 'zero':
            return 'Donation'
        else:
            return 'Unknown'
        
    def _cleanup_country_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up the country column by
        1. Mapping 'EIRE' to 'Ireland' (assuming correct).
        2. Mapping 'RSA' to 'South Africa (assuming correct)
        3. Mapping 'U.K' to 'United Kindgdom
        4. Mapping 'nan', 'Unspecified', 'West Indies' to 'Unknown'.
        5. Else, leave as is."""
        country_mapping = {
            "EIRE": "Ireland",
            "RSA": "South Africa",
            "U.K.": "United Kingdom",
            "Unspecified": "Unknown",
            "West Indies": "Unknown",
            'nan': "Unknown",
        }

        # replace values in the country column based on mapping
        df["country"] = df["country"].replace(country_mapping)

        return df
    
    def _upper_case_and_trim_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert stock codes to uppercase.
        Sometimes they are lowercase and sometimes upper.
        But from initial data profiling these seem to be the same product 
        because of the description.
        """
        df["code"] = df["code"].str.upper().str.strip()
        return df


    def run_pipeline(self, df: pd.DataFrame):
        """Run ETL pipeline to full-load invoices CSV file."""
        self.logger.info("Renaming columns inside DataFrame")
        df = self._rename_columns(df)

        self.logger.info("Cast columns inside DataFrame")
        df = self._cast_columns(df)

        self.logger.info("Performing data cleaning")
        df = self._perform_data_cleaning(df)

        self.logger.info("Creating 'type' column for different types of invoices")
        df = self._create_quantity_class_column(df)
        df = self._create_price_class_column(df)
        df["type"] = df.apply(self._categorize_invoice, axis=1)

        self.logger.info("Cleaning up customer country column")
        df = self._cleanup_country_column(df)

        self.logger.info("Uppercasing and trimming whitespace from stock codes")
        df = self._upper_case_and_trim_code(df)

        self.logger.info("Run date dimension etl step")
        date_dim = self.etl_date_dim.run_etl()

        self.logger.info("Run invoice dimension etl step")
        invoice_dim = self.etl_invoice_dim.run_etl(df=df)

        self.logger.info("Run customer dimension etl step")
        customer_dim = self.etl_customer_dim.run_etl(df=df)

        self.logger.info("Run product dimension etl step")
        product_dim = self.etl_product_dim.run_etl(df=df)

        self.logger.info("Run transaction fact etl step")
        self.etl_transaction_fact.run_etl(
            source_df=df,
            date_dim=date_dim,
            invoice_dim=invoice_dim,
            product_dim=product_dim,
            customer_dim=customer_dim
        )
