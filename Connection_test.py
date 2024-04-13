import os
import snowflake.connector
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {"account":"BDUHNMX-SD34895",
"user":"jleonard8812",
"password":
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"AZURE_TEST",
"schema":"dbt_test"
}

test_session = Session.builder.configs(connection_parameters).create()

print(test_session.sql("select current_warehouse(), current_database(), current_schema()").collect())

session = Session.builder.configs(connection_parameters).create()
