# import Session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import the data types from the snowflake.snowpark package
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# import the json library to read the connection parameters from a JSON file
import json
# assign the source file name to a variable
source_file_name = 'Chapter_06/Orders_2023-07-07.csv'

# read the credentials from a file
credentials = json.load(open('Chapter_06/connection_parameters.json'))
# create a dictionary with the connection parameters
connection_parameter_dict = {
    "account": credentials['account'],
    "user": credentials['user'],
    "password": credentials['password'],
    "role": credentials['role'],
    "warehouse": credentials['warehouse'],
    "database": credentials['database'],
    "schema": credentials['schema']
}
# create a session using the connection parameters
my_session = Session.builder.configs(connection_parameter_dict).create()

#put the file into the stage
result = my_session.file.put(source_file_name, "@orders_stage")
print(result)

#define the schema for the CSV file with fields Customer,Order_date,Delivery_date,Baked_good_type,Quantity
schema_for_csv = StructType([
    StructField("Customer", StringType()),
    StructField("Order_date", DateType()),
    StructField("Delivery_date", DateType()),
    StructField("Baked_good_type", StringType()),
    StructField("Quantity", DecimalType())])

# create a data frame from the CSV file in the stage
df = my_session.read.schema(schema_for_csv).csv("@orders_stage")

result = df.copy_into_table("ORDERS_STG", format_type_options ={"skip_header": 1})

my_session.close()