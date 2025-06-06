# import Session from snowflake.snowpark import Session
from snowflake.snowpark import Session
#create a dictionary with the connection parameters
connection_parameter_dict = {
    "account": "FTYYFQZ-PB91310",
    "user": "pratik",
    "password": "Clopes82@snowflake",
    "role": "sysadmin",
    "warehouse": "BAKERY_WH",
    "database": "BAKERY_DB",
    "schema": "SNOWPARK"
}

my_session = Session.builder.configs(connection_parameter_dict).create()
my_session.sql("SELECT CURRENT_VERSION()").show()
my_session.close()