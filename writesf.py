
  
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, max as max_
import os


from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType
from snowflake.snowpark.functions import substr

# Replace the below connection_parameters with your respective snowflake account,user name and password
connection_parameters = {"account":"BDUHNMX-SD34895",
"user":"jleonard8812",
"password": "Lilthuglife8812!!!",
"role":"ACCOUNTADMIN",
"warehouse":"COMPUTE_WH",
"database":"demo_db",
"schema":"public"
}

session = Session.builder.configs(connection_parameters).create()


schema = StructType([StructField("FIRST_NAME", StringType()),
StructField("LAST_NAME", StringType()),
StructField("EMAIL", StringType()),
StructField("ADDRESS", StringType()),
StructField("CITY", StringType()),
 StructField("DOJ",DateType())])

# employee_s3 = session.read.schema(schema).csv('@my_s3_stage/employee/')
# Use session.read.schema and session.read.csv and mention the command to read data from s3
# While reading make sure you ignore the bad records.
# author, cat , genre_s, id 
# employee_s3_orc = session.read.orc('@my_s3_stage/orc_folder/')
# employee_s3_orc = employee_s3_orc.select(col("$2").as_("id"), col("$3").as_("first_name"), 
#                                                  col("$4").as_("last_name"), col("$5").as_("email"))
# # employee_s3_orc.show()

# employee_s3_orc = employee_s3_orc.write.mode("append").save_as_table("int_emp_details_orc")

# Write data frame employee_s3 to employee table in snowflake.

session = Session.builder.configs(connection_parameters).create()


schema = StructType([
    StructField("Name", StringType()),
    StructField("Address", StringType()),
    StructField("City", StringType()),
    StructField("Pin", IntegerType()), # Assuming "pin" is a string
    StructField("Age", IntegerType()),
    StructField("Salary", IntegerType())
])

# # >>> # Create a temp stage.
# mystage = session.sql("create or replace temp stage mystage").collect()
# file_path = os.path.abspath("/Users/lilrawj/Downloads/Instruction/employee.csv")
# get_path = os.path.abspath("/Users/lilrawj/Downloads")
# # print(file_path) 
# # >>> # Upload a file to a stage.
# put_result = session.file.put(file_path, "@mystage/prefix1")
# get_result = session.file.get("@mystage/prefix1/employee.csv", get_path)
# put_result[0].status
# employee_local = session.read.schema(schema).option("skip_header",1).csv('@mystage/prefix1')
# employee_local.show()

# copy_table = employee_local.copy_into_table('employee')
# print(copy_table)

# Employee table does not contain DEPTCODE column
emp_stg_tbl = session.table("DEMO_DB.PUBLIC.EMPLY")
first_max_salary = emp_stg_tbl.select(max_("SALARY"))
second_max_salary = emp_stg_tbl.select("SALARY").where(~emp_stg_tbl["SALARY"].in_(first_max_salary.collect()[0][0])).agg(max_("SALARY"))
first_and_second = second_max_salary.union(first_max_salary)
# emp_dpt_join = emp_stg_tbl.join(emp_dpt_tbl, emp_stg_tbl["DEPTCODE"]==emp_dpt_tbl["DEPTCODE"] )


first_and_second.show()