import pandas as pd 
import great_expectations as gx
import great_expectations.expectations as gxe

def get_validator(df, name):
    context = gx.get_context()
    ds = context.data_sources.add_or_update_pandas(name=f"{name}_src")
    asset = ds.add_dataframe_asset(name=f"{name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite"))
    
    return batch_definition, suite, context

def run_validator(df, context, batch_definition, suite, name):
    validation_def = context.validation_definitions.add(gx.ValidationDefinition(
            name=f"{name}_validation",
            data=batch_definition,
            suite=suite))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
    name=f"{name}_checkpoint",
    validation_definitions=[validation_def]
    ))
    results = checkpoint.run(batch_parameters={"dataframe": df})
    context.build_data_docs()
    context.open_data_docs()
    return results

def expect_valid_states(suite, col):
    VALID_STATES = ["AC", "AL", "AP", "AM",
            "BA", "CE", "DF", "ES", "GO",
            "MA", "MT", "MS", "MG", "PA",
            "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC",
            "SP", "SE", "TO"
            ]
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column=col, value_set=VALID_STATES))

def v_raw_customers(df):
    CUSTOMERS_RAW_SCHEMA = {
    "customer_id": "str",
    "customer_unique_id": "str",
    "customer_zip_code_prefix": "int64",
    "customer_city": "str",
    "customer_state": "str",
    }

    batch_definition, suite, context = get_validator(df, "cust")

    
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
   
    for col, dtype in CUSTOMERS_RAW_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col,type_= dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_unique_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_zip_code_prefix"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="customer_zip_code_prefix", min_value=1000, max_value=99999, mostly=0.99
    ))

    expect_valid_states(suite, "customer_state")

    results = run_validator(df, context, batch_definition, suite, "cust")
    
    if not results.success:
        raise ValueError("Raw customers validation failed.")
    
    return results

def v_raw_geolocation(df):
    CUSTOMERS_RAW_SCHEMA = {
        "geolocation_city": "str",
        "geolocation_state": "str",
        "geolocation_zip_code_prefix": "int64",
        "geolocation_lng": "float64",
        "geolocation_lat": "float64"}
    batch_definition, suite, context = get_validator(df, "geo")
    for col, dtype in CUSTOMERS_RAW_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col,type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="geolocation_lat"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="geolocation_lng"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="geolocation_zip_code_prefix"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="geolocation_city", mostly=0.99))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="geolocation_state", mostly=0.95))
    
    expect_valid_states(suite, "geolocation_state")

    results = run_validator(df, context, batch_definition, suite, "geo")

    if not results.success: 
        raise("Raw geolocation validation failed.")
    
    return results 

def v_raw_orders(df):
    ORDERS_SCHEMA = {
        "order_id": "str",
        "customer_id": "str",
        "order_status": "str",
        "order_purchase_timestamp": "str",
        "order_approved_at": "str",
        "order_delivered_carrier_date": "str",
        "order_delivered_customer_date": "str",
        "order_estimated_delivery_date": "str",
    }
    VALID_STATUSES = ["delivered", "shipped", "canceled", "unavailable",
                      "invoiced", "processing", "created", "approved"]

    batch_definition, suite, context = get_validator(df, "orders")

    for col, dtype in ORDERS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_purchase_timestamp"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_delivered_customer_date", mostly=0.90))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_approved_at", mostly=0.95))

    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=VALID_STATUSES))

    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="order_id"))

    results = run_validator(df, context, batch_definition, suite, "orders")

    if not results.success:
        raise ValueError("Raw orders validation failed.")
    return results


def v_raw_order_items(df):
    ORDER_ITEMS_SCHEMA = {
        "order_id": "str",
        "order_item_id": "int64",
        "product_id": "str",
        "seller_id": "str",
        "shipping_limit_date": "str",
        "price": "float64",
        "freight_value": "float64",
    }

    batch_definition, suite, context = get_validator(df, "order_items")

    for col, dtype in ORDER_ITEMS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="product_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="seller_id"))

    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0))

    results = run_validator(df, context, batch_definition, suite, "order_items")
    if not results.success:
        raise ValueError("Raw order items validation failed.")
    return results


def v_raw_order_payments(df):
    PAYMENTS_SCHEMA = {
        "order_id": "str",
        "payment_sequential": "int64",
        "payment_type": "str",
        "payment_installments": "int64",
        "payment_value": "float64",
    }
    VALID_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]

    batch_definition, suite, context = get_validator(df, "payments")

    for col, dtype in PAYMENTS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="payment_value"))

    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0))

    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="payment_installments", min_value=1))

    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=VALID_PAYMENT_TYPES))

    results = run_validator(df, context, batch_definition, suite, "payments")
    if not results.success:
        raise ValueError("Raw payments validation failed.")
    return results


def v_raw_order_reviews(df):
    REVIEWS_SCHEMA = {
        "review_id": "str",
        "order_id": "str",
        "review_score": "int64",
        "review_comment_title": "str",
        "review_comment_message": "str",
        "review_creation_date": "str",
        "review_answer_timestamp": "str",
    }

    batch_definition, suite, context = get_validator(df, "reviews")

    for col, dtype in REVIEWS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="review_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="review_score"))

    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="review_score", min_value=1, max_value=5
    ))

    results = run_validator(df, context, batch_definition, suite, "reviews")
    if not results.success:
        raise ValueError("Raw reviews validation failed.")
    return results


def v_raw_products(df):
    PRODUCTS_SCHEMA = {
        "product_id": "str",
        "product_category_name": "str",
        "product_name_lenght": "int64",
        "product_description_lenght": "int64",
        "product_photos_qty": "int64",
        "product_weight_g": "float64",
        "product_length_cm": "float64",
        "product_height_cm": "float64",
        "product_width_cm": "float64",
    }

    batch_definition, suite, context = get_validator(df, "products")

    for col, dtype in PRODUCTS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="product_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="product_id"))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="product_category_name", mostly=0.95))

    for col in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, mostly=0.95))

    results = run_validator(df, context, batch_definition, suite, "products")
    if not results.success:
        raise ValueError("Raw products validation failed.")
    return results


def v_raw_sellers(df):
    SELLERS_SCHEMA = {
        "seller_id": "str",
        "seller_zip_code_prefix": "int64",
        "seller_city": "str",
        "seller_state": "str",
    }

    batch_definition, suite, context = get_validator(df, "sellers")

    for col, dtype in SELLERS_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="seller_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="seller_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="seller_zip_code_prefix"))

    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="seller_zip_code_prefix", min_value=1000, max_value=99999, mostly=0.99
    ))

    expect_valid_states(suite, "seller_state")

    results = run_validator(df, context, batch_definition, suite, "sellers")
    if not results.success:
        raise ValueError("Raw sellers validation failed.")
    return results


def v_raw_translation(df):
    TRANSLATION_SCHEMA = {
        "product_category_name": "str",
        "product_category_name_english": "str",
    }

    batch_definition, suite, context = get_validator(df, "translation")

    for col, dtype in TRANSLATION_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_=dtype))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=col))

    results = run_validator(df, context, batch_definition, suite, "translation")
    if not results.success:
        raise ValueError("Raw translation validation failed.")
    return results