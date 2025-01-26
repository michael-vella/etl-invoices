from sqlalchemy import Column, Integer, String, Date, DateTime
from etl.db.core import Base
from etl.constants import DIM_DATE_TABLE_NAME

class DateDimension(Base):
    __tablename__ = DIM_DATE_TABLE_NAME

    date_key = Column(Integer, nullable=False, primary_key=True)
    date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(String, nullable=False) # JANUARY/FEBRUARY/MARCH...
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(String, nullable=False) # MONDAY/TUESDAY/WEDNESDAY...
    _insert_txstamp = Column(DateTime, nullable=False)
