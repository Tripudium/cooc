from datetime import datetime
from typing import List, Union
import pytz
import calendar

def get_months(start_date: datetime, end_date: datetime) -> List[str]:
    """
    Given two datetime objects, generate a list of months between them as strings in 'MM' format.
    """
    months = []
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        months.append(current_date.strftime('%y%m'))
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    return sorted(months)

def nanoseconds(input: Union[str, datetime]) -> int:
    """
    Convert datetime or string input to return nanosecond UNIX timestamp (int).

    The input can by one of the following:
        - YYMMDD.HHMM
        - dt.datetime (with or without timezone)
        
        Example:
            >>> nanoseconds(dt.datetime(2018, 3, 20, 18, 30, tzinfo=pytz.timezone('America/Chicago')))
        1521588600000000000 
    """
    assert isinstance(input, datetime) or isinstance(input, str)
    if isinstance(input, str):
        try:
            input = datetime.strptime(input, "%y%m%d.%H%M")
        except ValueError:
            print("Input string not in correct format")
    # Deal with some timezone issues
    used_tz = input.tzinfo if input.tzinfo is not None else pytz.utc
    input = used_tz.localize(input.replace(tzinfo=None))
    time_tuple = input.utctimetuple()
    timestamp = 1000 * (calendar.timegm(time_tuple) * 1000 * 1000 + input.microsecond)
    return timestamp