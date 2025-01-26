from sqlalchemy import Column, Integer, String, DateTime
from etl.db.core import Base
from etl.constants import DIM_INVOICE_TABLE_NAME

class InvoiceDimension(Base):
    """Invoice dimension table."""
    __tablename__ = DIM_INVOICE_TABLE_NAME

    invoice_key = Column(Integer, nullable=False, primary_key=True)
    invoice_no = Column(String, nullable=False)
    type = Column(String, nullable=False)
    _insert_txstamp = Column(DateTime, nullable=False)
