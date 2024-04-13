import os
import snowflake.connector
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import pandas

connection_parameters = {"account":"BDUHNMX-SD34895",
"user":"jleonard8812",
"password": ,
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"AZURE_TEST",
"schema":"dbt_test"
}

session = Session.builder.configs(connection_parameters).create()
# session.sql("use warehouse compute_wh").collect()
# step 1
df_orders_info = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS")

# step 2
row_count = df_orders_info.count()
# print (row_count)
# step 3
df_orders_select = df_orders_info.select("O_ORDERKEY","O_ORDERSTATUS","O_TOTALPRICE")
# df_orders_select.show()
# df_orders_select.show(20)

# step #4
# df_pandas = df_orders_select.toPandas()
# df_stats= df_pandas.describe()
df_orders_select.describe().show()
# df_orders_info_cnt = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS").count()



# create a dataframe by inferring a schema from the data

# test = session.create_dataframe([1, 2, 3, 4], schema=["a"])
# test = session.create_dataframe([[1, 2, 3, 4],[5, 6, 7, 8]], schema=["a","b","c","d"])
# test = session.create_dataframe([[1, 2], [3,4], [None, 5]], schema=["a", "b"])


# schema = StructType([StructField("Name", StringType()), StructField("Salary", IntegerType()), StructField("Doj", Datetype())])
# type(test)

# test = session.create_dataframe([[1, 2, 3, 123],[1, 2, 3, "ABC"],[1, 2, 3, "HPC"],[1, 2, 3, "EMD"]], schema=["a","b","c","d"])
# test.show()

# test = session.create_dataframe([[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022'],[1, 2, 3, '26-01-2022']], schema=["a","b","c","d"])
# test.show()

# test = session.create_dataframe([[1, 2, 3, 26.897],[1, 2, 3, 27.897],[1, 2, 3, 29.897],[1, 2, 3, 39.897]], schema=["a","b","c","d"])
# test.show(1)

# test = session.create_dataframe([[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None],[1, 2, 3, None]], schema=["a","b","c","d"])
# test.show()

# test = session.create_dataframe([[1, 2, 3, {"a":"hi"}],[1, 2, 3, None],[1, 2, 3, {"a":"Bye"}],[1, 2, 3, {"a":"hello"}]], schema=["a","b","c","d"])
# test.show()

# test = session.create_dataframe([[1, 2, 3, ["Hi"]],[1, 2, 3, None],[1, 2, 3,["Hello"] ],[1, 2, 3, ["Namaste"]]], schema=["a","b","c","d"])

# test1 = test.cache_result()
# test1.show()
# type(test1)

# # Check performance

# begin = time.time()
# test.show()
# end = time.time()
# print(f"Total runtime of the program is {end - begin}")


# begin = time.time()
# test1.show()
# end = time.time()
# print(f"Total runtime of the program is {end - begin}")


