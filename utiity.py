import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2.extras


def generate_and_upload_to_db_new_daily_login_counts() -> None:
    NEGATIVE_NUMBER_LOGINS_ALLOWED = -10
    brainpop_db_connection = psycopg2.connect(host=os.environ['BRAINPOP_DB_HOST'],
                                              port=os.environ['BRAINPOP_DB_PORT'],
                                              user=os.environ['BRAINPOP_DB_USERNAME'],
                                              password=os.environ['BRAINPOP_DB_PASSWORD'],
                                              dbname=os.environ['BRAINPOP_DB_NAME'])
    with brainpop_db_connection:
        with brainpop_db_connection.cursor() as cursor:
            # get datetime adding new login counts for to db
            datetime_get_daily_counts = os.environ.get('EXECUTION_DATE')
            if datetime_get_daily_counts:
                datetime_get_daily_counts = datetime.strptime(datetime_get_daily_counts, '%Y-%m-%d').date()
            else:
                datetime_get_daily_counts = datetime.now().date() - timedelta(days=1)
            datetime_get_daily_counts_string = datetime_get_daily_counts.strftime('%Y-%m-%d')
            dt_year = datetime_get_daily_counts.year
            dt_month = datetime_get_daily_counts.month

            # get current monthly counts from monthly_login_aggregates table
            cursor.execute(generate_sql_statement_current_month_monthly_counts(dt_year, dt_month))
            monthly_counts = pd.DataFrame(cursor.fetchall(),
                                          columns=['user_type', 'user_id', 'device', 'month_num_logins'])

            if datetime_get_daily_counts.day > 1:
                # subtract daily counts of previous days using daily_login_aggregates from current monthly counts
                first_day_month_string = (datetime(dt_year, dt_month, 1)
                                          .strftime('%Y-%m-%d'))
                cursor.execute(generate_sql_statement_current_month_daily_counts(first_day_month_string))
                daily_counts = pd.DataFrame(cursor.fetchall(),
                                            columns=['user_id', 'device', 'num_logins_prev_days_months'])
                new_daily_counts = monthly_counts.merge(daily_counts, on=['user_id', 'device'], how='left')
                new_daily_counts['num_logins_prev_days_months'] = np.where(
                    pd.isna(new_daily_counts['num_logins_prev_days_months']),
                    0,
                    new_daily_counts['num_logins_prev_days_months'])
                new_daily_counts = new_daily_counts.assign(num_new_logins=lambda row: row['month_num_logins'] -
                                                                                      row[
                                                                                          'num_logins_prev_days_months'])
                # if sum of previous daily counts add up to more than current monthly counts, throw error
                if (new_daily_counts['num_new_logins'] < NEGATIVE_NUMBER_LOGINS_ALLOWED).any():
                    raise Exception(
                        'The summed number of logins in daily_login_aggregates for aleast one user_id/device'
                        f' for all days before {datetime_get_daily_counts_string} is more than the value for month'
                        f' in monthly_login_aggregates')
                new_daily_counts['num_new_logins'] = np.where(new_daily_counts['num_new_logins'] < 0, 0,
                                                              new_daily_counts['num_new_logins'])
            else:
                # getting daily counts for first day of month so all logins in monthly table attributed to that date
                new_daily_counts = monthly_counts
                new_daily_counts['num_new_logins'] = new_daily_counts['month_num_logins']

            # format data and insert into daily_login_aggregates
            new_daily_counts = (new_daily_counts
                                .assign(date=datetime_get_daily_counts.strftime('%Y-%m-%d'))
                                .filter(items=['date', 'user_type', 'user_id', 'num_new_logins', 'device']))
            psycopg2.extras.execute_values(cursor,
                                           'INSERT INTO daily_login_aggregates VALUES %s',
                                           list(new_daily_counts.itertuples(index=False, name=None)))


def generate_sql_statement_current_month_daily_counts(first_day_month_string: str) -> str:
    """
    generate sql statement to get sum of daily logins per user_id/device over month of current date inserting new logins into
    """
    current_month_daily_counts = f"""
        SELECT user_id, device, SUM(num_logins) AS num_logins
        FROM daily_login_aggregates
        WHERE date >= '{first_day_month_string}'
        GROUP BY user_id, device 
    """
    return current_month_daily_counts


def generate_sql_statement_current_month_monthly_counts(year: int, month: int) -> str:
    """
    generate sql statement to get rows from monthly_login_aggregates for month of current date inserting new logins into
    """
    month_string = f'{month}' if month >= 10 else f'0{month}'
    current_month_monthly_counts = f"""
        SELECT user_type, user_id, device, num_logins
        FROM monthly_login_aggregates
        WHERE year = {year} and month = '{month_string}'
    """
    return current_month_monthly_counts
