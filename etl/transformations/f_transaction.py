import pandas as pd
from etl.transformations.base import ETLBase
from etl.logger import get_logger
from sqlalchemy.orm import Session, sessionmaker
from etl.db.f_transaction import TransactionFact


class ETLTransactionFact(ETLBase):
    """ETL logic used to create transaction fact table"""
    def __init__(self, table_name: str, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.table_name: str = table_name
        self.db_session: sessionmaker[Session] = session

    def _select_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select only the required columns for the transaction fact table."""
        return df[["invoice_no", "type", "code", "invoice_date", "customer_id", "country", "quantity", "price"]]
    
    def _join_with_date_dim(self, df: pd.DataFrame, date_dim: pd.DataFrame) -> pd.DataFrame:
        """Join dataframe with date dim and drop redundant columns"""
        df["invoice_date"] = pd.to_datetime(df["invoice_date"]).dt.date
        date_dim["date"] = pd.to_datetime(date_dim["date"]).dt.date
        date_dim = date_dim[["date_key", "date"]]
        joined_df: pd.DataFrame = pd.merge(
            df,
            date_dim,
            left_on="invoice_date",
            right_on="date",
            how="left"
        )

        return joined_df.drop(columns=["date", "invoice_date"])
    
    def _join_with_invoice_dim(self, df: pd.DataFrame, invoice_dim: pd.DataFrame) -> pd.DataFrame:
        """Join dataframe with invoice dim and drop redundant columns"""
        invoice_dim = invoice_dim[["invoice_key", "invoice_no", "type"]]
        joined_df: pd.DataFrame = pd.merge(
            df,
            invoice_dim,
            on=["invoice_no", "type"],
            how="left"
        )

        return joined_df.drop(columns=["invoice_no", "type"])
    
    def _join_with_customer_dim(self, df: pd.DataFrame, customer_dim: pd.DataFrame) -> pd.DataFrame:
        """Join dataframe with customer dim and drop redundant columns"""
        customer_dim = customer_dim[["customer_key", "customer_id", "country"]]
        joined_df: pd.DataFrame = pd.merge(
            df,
            customer_dim,
            on=["customer_id", "country"],
            how="left"
        )

        return joined_df.drop(columns=["customer_id", "country"])
    
    def _join_with_product_dim(self, df: pd.DataFrame, product_dim: pd.DataFrame) -> pd.DataFrame:
        """Join dataframe with product dim and drop redundant columns"""
        product_dim = product_dim[["product_key", "code"]]
        joined_df: pd.DataFrame = pd.merge(
            df,
            product_dim,
            on="code",
            how="left"
        )

        return joined_df.drop(columns=["code"])
    
    def _assert_no_missing_dim_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assert that there are no integrity issues"""
        column_names = ["date_key", "invoice_key", "product_key", "customer_key"]

        # check for missing values in any of the dimension key columns
        missing_keys = df[column_names].isnull().sum()
        
        # if any column has missing values, raise an exception
        if missing_keys.any():
            missing_cols = missing_keys[missing_keys > 0]
            raise ValueError(f"Missing values found in the following columns: {', '.join(missing_cols.index)}")
        
        return df
    
    def _group_to_fact_grain(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group to fact grain as there are some duplicate entries"""
        grouped_df = df.groupby(
            ["date_key", "customer_key", "invoice_key", "product_key"]
        )[['quantity', 'price']].sum().reset_index()

        return grouped_df

    def _create_transaction_fact(
        self,
        source_df: pd.DataFrame,
        date_dim: pd.DataFrame,
        invoice_dim: pd.DataFrame,
        product_dim: pd.DataFrame,
        customer_dim: pd.DataFrame
    ) -> pd.DataFrame:
        """Create transaction fact table"""
        df: pd.DataFrame = self._select_required_columns(source_df)

        # join with date dim
        final_df: pd.DataFrame = self._join_with_date_dim(
            df=df,
            date_dim=date_dim
        )

        # join with invoice dim
        final_df = self._join_with_invoice_dim(
            df=final_df,
            invoice_dim=invoice_dim
        )

        # join with customer dim
        final_df = self._join_with_customer_dim(
            df=final_df,
            customer_dim=customer_dim
        )

        # join with product dim
        final_df = self._join_with_product_dim(
            df=final_df,
            product_dim=product_dim
        )

        # assert no missing dim keys
        final_df = self._assert_no_missing_dim_keys(final_df)

        final_df = self._group_to_fact_grain(final_df)
        
        return final_df

    def run_etl(
        self,
        source_df: pd.DataFrame,
        date_dim: pd.DataFrame,
        invoice_dim: pd.DataFrame,
        product_dim: pd.DataFrame,
        customer_dim: pd.DataFrame
    ) -> None:
        """Concrete implementation of run_etl abstract method."""
        self.logger.info("Truncating table for full-load")
        self.truncate_table(table_name=self.table_name, session=self.db_session)

        self.logger.info("Creating transaction fact table in pandas")
        df: pd.DataFrame = self._create_transaction_fact(
            source_df=source_df,
            date_dim=date_dim,
            invoice_dim=invoice_dim,
            product_dim=product_dim,
            customer_dim=customer_dim
        )
        df = self.create_insert_txstamp(df=df)

        self.logger.info("Inserting dataframe into table")
        records = df.to_dict(orient="records")
        self.db_session.bulk_insert_mappings(TransactionFact, records)

        self.logger.info("Transaction fact table ETL step successful")
