from sqlalchemy import Column, Integer, String, DateTime, Double
from etl.db.core import Base
from etl.constants import FACT_TRANSACTION_TABLE_NAME

class TransactionFact(Base):
    __tablename__ = FACT_TRANSACTION_TABLE_NAME

    date_key = Column(Integer, nullable=False, primary_key=True)
    invoice_key = Column(Integer, nullable=False, primary_key=True)
    product_key = Column(Integer, nullable=False, primary_key=True)
    customer_key = Column(Integer, nullable=False, primary_key=True)
    quantity = Column(Integer, nullable=False)
    price = Column(Double, nullable=False)
    _insert_txstamp = Column(DateTime, nullable=False)
