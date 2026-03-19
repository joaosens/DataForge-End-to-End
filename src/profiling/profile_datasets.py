import numpy as np

def prof_customers(df) -> None:
    print("CUSTOMERS PROFILE\n")
    #np.set_printoptions(threshold=np.inf) --If needs array cities inspection in your similaritys,inconsistences... 
    #print('\nCustomer Cities:\n',sorted(df["customer_city"].unique()))
    print('\nCustomer States:\n', df["customer_state"].nunique()) # Twenty seven States this corresponds to Brazilian States.
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)
def prof_geolocation(df) -> None:
    print("GEOLOCATION PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nDuplicateds Zip Code:\n', df.duplicated(subset=["geolocation_zip_code_prefix"]).sum())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)
def prof_order_items(df) -> None:
    print("ORDER ITEMS PROFILE\n")
    print('\nHead:\n', df.head(15))
    print('\nIs Null Sum:\n', df.isnull().sum())
    print('\nDuplicateds Sum:\n', df.duplicated().sum())
    print('\nInfo:\n', df.info())
    print('\nColumns:\n', df.columns)
