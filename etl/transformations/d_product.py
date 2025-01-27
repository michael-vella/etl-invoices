import pandas as pd
import re
from etl.transformations.base import ETLBase
from etl.logger import get_logger
from sqlalchemy.orm import Session, sessionmaker
from etl.db.d_product import ProductDimension


class ETLProductDimension(ETLBase):
    """ETL logic used to create product dimension"""
    def __init__(self, table_name: str, session: sessionmaker[Session]):
        self.logger = get_logger(self.__class__.__name__)
        self.table_name: str = table_name
        self.db_session: sessionmaker[Session] = session

    def _select_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select only the required columns for the product dimension."""
        return df[["code", "description"]]
    
    def _deduplicate_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """For each code, select the description with the highest count"""
        # group by stock_code and description, then count occurrences
        count_df = df.groupby(["code", "description"], as_index=False).size()

        # sort be stock_code (asc) and size (desc)
        # then drop_duplicates will keep only the first appearance
        # first appearance should be the one with the highest count due to sort
        top_descriptions = (
            count_df.sort_values(["code", "size"], ascending=[True, False])
            .drop_duplicates(subset=["code"])
        )

        return top_descriptions.drop(columns=["size"])
    
    def _uppercase_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """Upper case description column for consistency"""
        df["description"] = df["description"].str.upper()
        return df
    
    def _cleanup_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up descriptiom column
        1. Convert NaN to UNKNOWN.
        2. Remove any non alphanumeric characters.
        """
        # convert 'NAN' string to 'UNKNOWN'
        df["description"] = df["description"].replace({"NAN": "UNKNOWN"})

        # trim whitespace
        df["description"] = df["description"].str.strip()
        
        # remove non-alphanumeric characters using regex
        df["description"] = df["description"].apply(
            lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x) if isinstance(x, str) else x
        )
        
        return df

    def _create_product_dim(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create product dimension"""
        df = self._select_required_columns(df)
        distinct_df: pd.DataFrame = self._deduplicate_description(df)
        processed_df: pd.DataFrame = self._uppercase_description(distinct_df)
        processed_df = self._cleanup_description(processed_df)
        processed_df = self.create_surrogate_key(surrogate_key_name="product_key", df=processed_df)
        return distinct_df

    def run_etl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Concrete implementation of run_etl abstract method."""
        self.logger.info("Truncating table for full-load")
        self.truncate_table(table_name=self.table_name, session=self.db_session)

        self.logger.info("Creating product dimension in pandas")
        df: pd.DataFrame = self._create_product_dim(df=df)
        df = self.create_insert_txstamp(df=df)

        self.logger.info("Inserting dataframe into table")
        records = df.to_dict(orient="records")
        self.db_session.bulk_insert_mappings(ProductDimension, records)

        self.logger.info("Product dimension ETL step successful")

        return df
