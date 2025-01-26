import pandas as pd
from etl.db.core import DBContext
from etl.constants import DB_SERVER, DB_USERNAME, DB_PASSWORD
from etl.pipeline import ETLPipeline
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from logger import get_logger


def _read_csv_from_source(file_path: str) -> pd.DataFrame:
    """Method to read from CSV file and return pandas DataFrame."""
    return pd.read_csv(
        file_path,
        sep=",",
        encoding="unicode_escape", # was required because of unicode character issues when reading the file
    )

def main():
    logger = get_logger("Main")
    
    logger.info("Initialising variables required.")
    database_name: str = "invoices"
    csv_file_path: str = "data\Invoices_Year_2009-2010.csv"

    db: DBContext = DBContext()

    # create database (if not exists)
    db.create_db(
        server=DB_SERVER,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        db_name=database_name
    )

    # connect to database using SQLAlchemy 
    engine = db.get_engine(
        server=DB_SERVER,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        db_name=database_name
    )

    # create tables
    db.create_tables(engine)

    # read csv file
    logger.info("Reading CSV file from source")
    df: pd.DataFrame = _read_csv_from_source(file_path=csv_file_path)

    # create session - so that we handle the ETL pipeline as one transaction
    # need to handle as one transcation because we will do a full-load approach
    # full-load approach will truncate the tables and re-create them, hence why we need one transaction
    Session = sessionmaker(bind=engine)
    session = Session()

    # run etl pipeline
    try:
        pipeline = ETLPipeline(session)
        pipeline.run_pipeline(df)
        session.commit()
    except SQLAlchemyError as e:
        logger.critical(f"Error running ETL pipeline: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()