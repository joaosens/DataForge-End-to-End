from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import src.transform.treatment as tr
import src.validation.validate_raw as vr
from importlib import import_module


with DAG(
dag_id='dataforge_pipeline',
schedule='@daily',
start_date=days_ago(1),
catchup=False
) as dag:
    @task
    def ingest_all():
        INGEST_MODULES=[
        ("ingest_customers", "customers"),
        ("ingest_geolocation", "geolocation"),
        ("ingest_order_items", "order_items"),
        ("ingest_order_payments", "payments"),
        ("ingest_order_reviews", "reviews"),
        ("ingest_orders", "orders"),
        ("ingest_products", "products"),
        ("ingest_products_translation", "translation"),
        ("ingest_sellers", "sellers")]

        df_ing={} 

        for mod_name, func_name in INGEST_MODULES:
            mod = import_module(f"src.ingestion.{mod_name}")
            func = getattr(mod, func_name)
            df_ing[func_name] = func()
        return df_ing
        
    df_ing = ingest_all()

    @task
    def raw_validate_all(df_ing: dict) -> dict:
        VALIDATE_MAP = {
        "v_raw_customers": "customers",
        "v_raw_geolocation": "geolocation",
        "v_raw_orders": "orders",
        "v_raw_order_items": "order_items",
        "v_raw_order_payments": "payments",
        "v_raw_order_reviews": "reviews",
        "v_raw_products": "products",
        "v_raw_sellers": "sellers",
        "v_raw_translation": "translation",
    }
        df_vr = {} 

        for func_name, df_key in VALIDATE_MAP.items():
            func = getattr(vr, func_name)
            df_vr[df_key] = func(df_ing[df_key])
        return df_vr
    
    df_vr = raw_validate_all(df_ing)

    for key, result in df_vr.items():
        print(f"{key}: {result.success}")

    @task
    def transform_all(df_ing: dict) -> dict:
        TRANSFORM_MAP = {
        "t_customers": "customers",
        "t_geolocation": "geolocation",
        "t_orders": "orders",
        "t_order_items": "order_items",
        "t_order_payments": "payments",
        "t_order_reviews": "reviews",
        "t_products": "products",
        "t_sellers": "sellers",
        "t_translation": "translation",
        }

        df_tr = {} 

        for func_name, df_key in TRANSFORM_MAP.items():
            func = getattr(tr, func_name)
            df_tr[df_key]= func(df_ing[df_key])
        return df_tr
    
    df_tr = transform_all(df_ing)

    @task
    def load_all(df_tr: dict) -> dict:

