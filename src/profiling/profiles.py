import pandas as pd

def prof_customers(df) -> None:
    print("CUSTOMERS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Customer IDs:\n', df["customer_id"].nunique())
    print('\nUnique Customer Unique IDs:\n', df["customer_unique_id"].nunique())  # If were same of the last, same customer did make more order. 
    print('\nCustomer States:\n', df["customer_state"].nunique())  # Must be 27.
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_geolocation(df) -> None:
    print("GEOLOCATION PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nDuplicateds Zip Code:\n', df.duplicated(subset=["geolocation_zip_code_prefix"]).sum())
    print('\nUnique Zip Codes:\n', df["geolocation_zip_code_prefix"].nunique())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_order_items(df) -> None:
    print("ORDER ITEMS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Orders:\n', df["order_id"].nunique())
    print('\nUnique Products:\n', df["product_id"].nunique())
    print('\nUnique Sellers:\n', df["seller_id"].nunique())
    print('\nPrice Stats:\n', df["price"].describe())          # Detects Price's Outliers
    print('\nFreight Stats:\n', df["freight_value"].describe()) # Detects Freight's Outliers
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_orders(df) -> None:
    print("ORDERS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Orders:\n', df["order_id"].nunique())       # Must be equal the total rows
    print('\nOrder Status Distribution:\n', df["order_status"].value_counts())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_order_payments(df) -> None:
    print("ORDER PAYMENTS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nPayment Type Distribution:\n', df["payment_type"].value_counts())  # Methods most useds 
    print('\nPayment Value Stats:\n', df["payment_value"].describe())           # Detects Value Outliers
    print('\nInstallments Stats:\n', df["payment_installments"].describe())     # Inspection with math stats of installments
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_order_reviews(df) -> None:
    print("ORDER REVIEWS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nReview Score Distribution:\n', df["review_score"].value_counts()) 
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_products(df) -> None:
    print("PRODUCTS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Products:\n', df["product_id"].nunique())
    print('\nUnique Categories:\n', df["product_category_name"].nunique())
    print('\nCategory Distribution:\n', df["product_category_name"].value_counts().head(10))  # Top 10 Categorys
    print('\nWeight Stats:\n', df["product_weight_g"].describe())   # Detects Outliers like absurd heavy weights
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_sellers(df) -> None:
    print("SELLERS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Sellers:\n', df["seller_id"].nunique())
    print('\nSeller States:\n', df["seller_state"].nunique())
    print('\nSeller State Distribution:\n', df["seller_state"].value_counts())  # Which's the region of most sellers concentrated?
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)

def prof_translation(df) -> None:
    print("TRANSLATION PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nUnique Categories:\n', df["product_category_name"].nunique())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)