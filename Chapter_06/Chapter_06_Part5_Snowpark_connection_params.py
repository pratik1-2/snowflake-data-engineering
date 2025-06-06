#import session from the snowflake.snowpark package
from snowflake.snowpark import Session
#import the json library to read the connection parameters from a JSON file
import json
#read the connection parameters from a JSON file
credentials = json.load(open('Chapter_06/connection_parameters.json'))
#ccreate a dictionary with the connection parameters
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
#close the session
# select the current timestamp from the session
ts = my_session.sql("SELECT CURRENT_TIMESTAMP()").collect()
#print the current timestamp to console
print(ts)
my_session.close()