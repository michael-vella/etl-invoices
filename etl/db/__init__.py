# These imports are required so that when we initialise the 'db' module, 
# the inheritance from the 'Base' SQLAlchemy class occurs.

# If we don't import these, when we run the 'create_tables' method, 
# nothing will be created unless specifically imported somewhere else.

from etl.db.d_customer import CustomerDimension
from etl.db.d_invoice import InvoiceDimension
from etl.db.d_date import DateDimension
from etl.db.d_product import ProductDimension
from etl.db.f_transaction import TransactionFact
