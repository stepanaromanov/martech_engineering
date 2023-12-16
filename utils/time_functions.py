import datetime
from datetime import date


# this function creates name for csv to export using current datetime and dataframe name
def get_current_datetime():
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# this function creates date range in string format from particular week number in the year
def get_date_range_from_week(p_year, p_week):
    first_day_of_week = datetime.datetime.strptime(f'{p_year}/W{int(p_week)}/1', "%Y/W%W/%w").date()
    last_day_of_week = first_day_of_week + datetime.timedelta(days=6.9)
    return f'{first_day_of_week.strftime("%Y/%m/%d")} - {last_day_of_week.strftime("%Y/%m/%d")}'


# this function returns start date and end date for google analytics (timedelta=90 days)
def get_timedelta_90_days():
    start_date = date.today() - datetime.timedelta(90)
    end_date = date.today()
    return start_date, end_date