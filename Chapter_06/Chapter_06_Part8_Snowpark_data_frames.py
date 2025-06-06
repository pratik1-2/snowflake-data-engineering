# import session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import the json library to read the connection parameters from a JSON file
import json

#read the connection parameters from a JSON file
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

# retrieve tables into data frames
df_orders = my_session.table("ORDERS_STG")
df_dim_date = my_session.table("DIM_DATE")

# join tables into dataframe
df_orders_with_holiday_flag = df_orders.join(
    df_dim_date, df_orders["delivery_date"] == df_dim_date["day"], "left")

# create a view from the joined dataframe
df_orders_with_holiday_flag.create_or_replace_view("ORDERS_HOLIDAY_FLG")

# close the session
my_session.close()