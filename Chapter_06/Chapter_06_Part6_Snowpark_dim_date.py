# import session from the snowflake.snowpark package
from snowflake.snowpark import Session
# import data types from the snowflake.snowpark package
from snowflake.snowpark.types import DateType, BooleanType , StructType, StructField
# import the json library to read the connection parameters from a JSON file
import json
# import date and timedelata from the datetime library for generating dates
from datetime import date, timedelta
# Install the holidays package using pip
# pip install holidays
# import the holidays package to determine if a date is a holiday
import holidays

# define a function that returns True if p_date is a holiday in p_country
def is_holiday(p_date: date, p_country: str) -> bool:
    # get a list of holidays in the specified country
    all_holidays = holidays.country_holidays(p_country)
    # return True if p_date is a holiday, otherwise return False
    if p_date in all_holidays:
        return True
    else:
        return False
    
# generating a list of dates starting from the start_date followed by as many days as defined in the no_days variable
#define the start date
start_date = date(2023, 1, 1)
# define the number of days to generate
no_days = 731
# store consecutive dates in a list
dates = [start_date + timedelta(days=i) for i in range(no_days)]

# create a list of lists that combines the list of dates
# with the output of the is_holiday function
holiday_flags = [[d, is_holiday(d, 'US')] for d in dates]

print(holiday_flags)

# establish a connection with Snowflake because we need the Snowpark API
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

#Listing 6.9
# create a data frame from the holiday_flags list of lists and define the schema as two columns:
# - column named "day" with data type DateType
# - column named "holiday_flg" with data type BooleanType
df = my_session.create_dataframe(
    holiday_flags,
    schema=StructType([
        StructField("day", DateType()),
        StructField("holiday_flg", BooleanType())
    ]))
# print the schema of the data frame
df.print_schema()
print(df.collect())
#Listing 6.10
# save the data frame to a Snowflake table named DIM_DATE and overwrite the table if it already exists
df.write.mode("overwrite").save_as_table("DIM_DATE")

my_session.close()  # close the session