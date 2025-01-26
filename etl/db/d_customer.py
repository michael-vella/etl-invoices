from sqlalchemy import Column, Integer, String, DateTime
from etl.db.core import Base
from etl.constants import DIM_CUSTOMER_TABLE_NAME

class CustomerDimension(Base):
    """Customer dimension table."""
    __tablename__ = DIM_CUSTOMER_TABLE_NAME

    customer_key = Column(Integer, nullable=False, primary_key=True)
    customer_id = Column(Integer, nullable=False)
    country = Column(String, nullable=False)
    _insert_txstamp = Column(DateTime, nullable=False)
