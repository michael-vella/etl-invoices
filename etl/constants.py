from dotenv import load_dotenv
import os

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DIM_INVOICE_TABLE_NAME = "dim_invoice"
DIM_PRODUCT_TABLE_NAME = "dim_product"
DIM_DATE_TABLE_NAME = "dim_date"
DIM_CUSTOMER_TABLE_NAME = "dim_customer"
FACT_TRANSACTION_TABLE_NAME = "fact_transactions"