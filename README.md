#Brainpop Take Home

### Setup & Environmental Variables

Run following to create database and initialize tables and load monthly and daily counts data
```
createdb ${BRAINPOP_DB_NAME}
BRAINPOP_DB_NAME=${BRAINPOP_DB_NAME} BRAINPOP_DB_USERNAME=${BRAINPOP_DB_USERNAME} make initialize-db
```

BRAINPOP_DB_NAME and BRAINPOP_DB_USERNAME are two of the five db environmental variables that must be set. See sample.env for the other three. The 

### Pipeline Strategy 

In order to build a pipeline that relied on the existing infrastructure, i.e. used the monthly_login_aggregates table,
I made an assumption based on the columns provided. The assumption was
   + The monthly_login_aggregates table is updated every time an user logins, i.e. in real time, instead of being updated once a month
     or at some other interval. I believe this was a reasonable assumption to make b/c of the d_last_accessed column, which I assume would need to be updated
     every time an user logins.   
        
Based on this assumption, I came up with the following strategy
+ Each day, at midnight, run code that would do the following
    + If the day that just finished was the first day of the month, create new rows in daily_login_aggregates with num_login values equal
      to num_logins of that month in monthly_login_aggregates
    + Otherwise, to get the number of logins for the day that just finished, subtract the summed number of logins from daily_login_aggregates for all
      previous days of month from the value in monthly_login_aggregates
      + A check is built into this. If the summed number of logins is greater than the monthly count by more than a value of X, then an exception is thrown (see Minor Challenge below for why would not just check is strictly greater).
      
The ```generate_and_upload_to_db_new_daily_login_counts``` function in ```utility.py``` is the function that would be run. I would run it in an Airflow DAG using
a PythonOperator and use the templating available to the operator to make the SQL statements used stand out more, i.e. I would be able to put the SQL statements in .sql files.

#### Challenges
There is one minor and major challenge with this strategy
+ Minor Challenge: Since this code would not be running exactly at midnight, any logins from midnight to the time is when the sql statement getting the monthly counts is run would be included in previous day totals. However, b/c of when this code would be run and what should be very short time period, this should not be major issue
+ Major Challenge: This strategy is not very resisilent to runtime failures. If the DAG fails to run, then instead of just having a very short period of time where current day logins would be included in previous day logins, that time
period would be from midnight to whenever the code issue is resolved and the DAG is rerun. While this double counting would be caught by the check above and that would help contain the number of days with data quality issues, there would still be multiple days with data quality issues, which would be problematic. Additionally, it would not be possible to validate previous days or go back and rerun previous days. 
Two solutions to this are:
    + **monthly_login_aggregates table snapshots: As part of this DAG, store in S3 or some other fashion (could create one table for each day of month) a snapshot of all the rows for monthly_login_aggregates for current month. Keep the snapshots for the entire month and then delete the snapshots/truncate db tabled at end of month. You could use this to both figure out the daily totals if the code fails to run or do much more robust testing by comparing
      the values in daily_login_aggregates to the expected values calculated by comparing adjcanet snapshots. Code would need to be added for this strategy to be utilized instead of the primary strategy whenever manully kicking off the DAG. Alternately, this could be made the primary strategy.** (I thought about this idea at end and now realize it is better strategy than my initial one) 
    + kafka or other streaming solution: This would be the most robust solution but would require much more change to the data collection process than what I have previously discussed. Can implement a kafka topic where each individual login is recorded and then at midnight, all records with previous day date are counted.
    
#### Running My Code
To see the results of my code, run index.py with the EXECUTION_UTC env variable set to any day in June 2020. This will result in
25 rows being added to daily_login_aggregates, one for each user for that date, with some num_logins equal to num_logins in monthly_login_aggregates for
users I didn't add any rows to daily_login_aggregates for
    