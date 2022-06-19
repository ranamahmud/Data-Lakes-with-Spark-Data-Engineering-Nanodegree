# Data Engineering Nanodegree Udacity Project
## Summary
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I've built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## How to run the Python scripts

1. Install pyspark by running the following command in terminal
```pip install pyspark```
2. Add aws AWS_ACCESS_KEY_ID and AWS_SECRET_KEY in dl.cfg file.
3. Run the following command in terminal 
   ```python etl.py```
## Files in the repository
ETL pipeline: etl.py
Instructions file: README.md
Configuration file dl.cfg