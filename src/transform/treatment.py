import pandas as pd
from src.transform.utils import normalize_cities

def t_customers(df) -> pd.DataFrame:
    df["city_final"] = normalize_cities(df, "customer_city", "customer_state")
    return df 

def t_geolocation(df) -> pd.DataFrame:
    df["city_final"] = normalize_cities(df, "geolocation_city", "geolocation_state")
    df = df.groupby("geolocation_zip_code_prefix", as_index=False).agg(
        geolocation_lat=("geolocation_lat", "mean"),
        geolocation_lng=("geolocation_lng", "mean"),
        geolocation_city=("city_final", lambda x: x.mode()[0]),
        geolocation_state=("geolocation_state", lambda x: x.mode()[0]),
    )
    return df

def t_orders(df) -> pd.DataFrame:
    DATE_COLS = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in DATE_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    df["delivery_days"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    df["delay_days"] = (
        df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
    ).dt.days

    return df

def t_order_items(df) -> pd.DataFrame:
    df["shipping_limit_date"] = pd.to_datetime(df["shipping_limit_date"], errors="coerce")
    df["total_value"] = df["price"] + df["freight_value"]
    return df

def t_order_payments(df) -> pd.DataFrame:
    df["is_installment"] = df["payment_installments"] > 1
    return df

def t_order_reviews(df) -> pd.DataFrame:
    df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce")
    df["review_answer_timestamp"] = pd.to_datetime(df["review_answer_timestamp"], errors="coerce")

    df["response_hours"] = (
        df["review_answer_timestamp"] - df["review_creation_date"]
    ).dt.total_seconds() / 3600

    df["is_positive"] = df["review_score"] >= 4
    return df

def t_products(df) -> pd.DataFrame:
    df["product_category_name"] = (
        df["product_category_name"]
        .str.replace("_", " ")
        .str.strip()
    )
    df["product_volume_cm3"] = (
        df["product_length_cm"] *
        df["product_height_cm"] *
        df["product_width_cm"]
    )
    return df

def t_sellers(df) -> pd.DataFrame:
    df["seller_city"] = normalize_cities(df, "seller_city", "seller_state")
    return df

def t_translation(df) -> pd.DataFrame:
    df["product_category_name"] = df["product_category_name"].str.replace("_", " ").str.strip()
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("_", " ").str.strip()
    return df