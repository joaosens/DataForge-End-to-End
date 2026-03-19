from pathlib import Path
from src.ingestion.ingest_customers import customers
from src.ingestion.ingest_geolocation import geolocation
from src.ingestion.ingest_order_items import order_items
from src.validation.validate_raw import v_raw_customers
import src.transform.treatment as trans
import src.profiling.profile_datasets as prof



def main() -> None:
    df_cust = customers()
    #df_geo = geolocation()
    #df_order_items = order_items()
    v_raw_cust = v_raw_customers(df_cust)
    #prof.prof_customers(df_cust)
    #prof.prof_geolocation(df_geo)
    #prof.prof_order_items(df_order_items)
    #trans.t_geolocation(df_geo)

if __name__ == "__main__":
    main()