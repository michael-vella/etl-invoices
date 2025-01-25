import pandas as pd


df: pd.DataFrame = pd.read_csv(
    "..\data\Invoices_Year_2009-2010.csv",
    sep=",",
    encoding="unicode_escape", # was required because of unicode character issues reading the file.
)

prcsd_df: pd.DataFrame = df.rename(
    columns={
        "Invoice": "invoice_code",
        "StockCode": "product_code",
        "Description": "product_description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "Price": "price",
        "Customer ID": "customer_id",
        "country": "customer_country",
    }
)

prcsd_df["invoice_code"] = prcsd_df["invoice_code"].astype(str)
prcsd_df["product_code"] = prcsd_df["product_code"].astype(str)
prcsd_df["product_description"] = prcsd_df["product_description"].astype(str)
prcsd_df["quantity"] = prcsd_df["quantity"].astype(int)
# prcsd_df["invoice_date"] = pd.to_datetime(prcsd_df["invoice_date"])
prcsd_df["price"] = prcsd_df["price"].astype(float)
# prcsd_df["customer_id"] = prcsd_df["customer_id"].astype(int)
# prcsd_df["customer_country"] = prcsd_df["customer_country"].astype(str)

# Create new columns to classify quantity and quality
prcsd_df['quantity_class'] = prcsd_df['quantity'].apply(lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'zero'))
prcsd_df['price_class'] = prcsd_df['price'].apply(lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'zero'))

# Group by the new classification columns and count the number of rows in each combination
result = prcsd_df.groupby(['quantity_class', 'price_class']).size().reset_index(name='count')

print(result)

neg_pos_df = prcsd_df[(prcsd_df['quantity_class'] == 'negative') & (prcsd_df['price_class'] == 'positive')]
neg_zero_df = prcsd_df[(prcsd_df['quantity_class'] == 'negative') & (prcsd_df['price_class'] == 'zero')]
pos_neg_df = prcsd_df[(prcsd_df['quantity_class'] == 'positive') & (prcsd_df['price_class'] == 'negative')]
pos_pos_df = prcsd_df[(prcsd_df['quantity_class'] == 'positive') & (prcsd_df['price_class'] == 'positive')]
pos_zero_df = prcsd_df[(prcsd_df['quantity_class'] == 'positive') & (prcsd_df['price_class'] == 'zero')]

columns_to_select = ["product_code", "invoice_code", "product_description", "quantity", "price"]

print()
print("Quantity: Negative. Price: Positive")
print(neg_pos_df[columns_to_select])
print()

print()
print("Quantity: Negative. Price: Zero")
print(neg_zero_df[columns_to_select])
print()

print()
print("Quantity: Positive. Price: Negative")
print(pos_neg_df[columns_to_select])
print()

print()
print("Quantity: Positive. Price: Positive")
print(pos_pos_df[columns_to_select])
print()

print()
print("Quantity: Positive. Price: Zero")
print(pos_zero_df[columns_to_select])
print()


# We have the following scenarios:

# Quantity|Price   |Count  |Invoice Type

# Negative|Positive|10,205 |Goods Bought
# Negative|Zero    |2,121  |Free Goods Received
# Positive|Negative|4      |Adjustments
# Positive|Positive|511,537|Goods Sold
# Positive|Zero    |1,594  |Free Goods Given