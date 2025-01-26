from sqlalchemy import Column, Integer, String, DateTime
from etl.db.core import Base
from etl.constants import DIM_PRODUCT_TABLE_NAME

class ProductDimension(Base):
    """Product dimension table."""
    __tablename__ = DIM_PRODUCT_TABLE_NAME

    product_key = Column(Integer, nullable=False, primary_key=True)
    code = Column(String, nullable=False)
    description = Column(String, nullable=False)
    _insert_txstamp = Column(DateTime, nullable=False)
