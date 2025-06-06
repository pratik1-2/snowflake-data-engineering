# The Snowpark package is required for Python Worksheets
import snowflake.snowpark as snowpark
# Adding the holidays package
import holidays

def is_holiday(session: snowpark.Session):
   # get a list of holidays in the US
   all_holidays = holidays.country_holidays('US')
   # return TRUE if January 1, 2024 is a holiday, otherwise return false
   if '2024-01-01' in all_holidays:
       return True
   else:
       return False