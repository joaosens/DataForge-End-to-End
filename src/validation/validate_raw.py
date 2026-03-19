import pandas as pd 
import great_expectations as gx
import great_expectations.expectations as gxe

def get_validator(df, source_name, asset_name, suite_name):
    context = gx.get_context()
    ds = context.data_sources.add_or_update_pandas(name=source_name)
    asset = ds.add_dataframe_asset(name=asset_name)
    batch_definition = asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))
    
    return batch_definition, suite, context

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

    batch_definition, suite, context = get_validator(df, "cust_src", "cust_asset", "cust_suite")

    
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
   
    for col, dtype in CUSTOMERS_RAW_SCHEMA.items():
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col,type_= dtype))

    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="customer_unique_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="customer_zip_code_prefix",
        min_value=1000,
        max_value=99999,
        mostly=0.95
    ))

    expect_valid_states(suite, "customer_state")

    validation_def = context.validation_definitions.add(gx.ValidationDefinition(
            name="cust_validation",
            data=batch_definition,
            suite=suite))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
    name="cust_checkpoint",
    validation_definitions=[validation_def]
    ))
    results = checkpoint.run(batch_parameters={"dataframe": df})
    context.build_data_docs()
    context.open_data_docs()

    if not results.success:
        raise ValueError("Raw customers validation failed.")
    
    return results