import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # TODO: get filepath to song data file
    song_data = config['S3']['song_data']
    # TODO: read song data file
    df = spark.read.json(song_data)

    # TODO: extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id", "year", "duration")
    
    # TODO: write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data+"songs_table.parquet")

    # TODO: extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location", "artist_latitude", "artist_longitude")
    
    # TODO: write artists table to parquet files
    artists_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data+"artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # TODO: get filepath to log data file
    log_data = config['S3']['log_data']

    # TODO: read log data file
    df = spark.read.json(log_data)
    
    # TODO: filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # TODO: extract columns for users table    
    artists_table = df.select("userId","firstName","lastName", "gender", "level")
    
    # TODO: write users table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists_table_log.parquet")

    # TODO: create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # TODO: create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # TODO: extract columns to create time table
    # start_time, hour, day, week, month, year, weekday

    df = df.withColumn("start_time", df.datetime)
    df = df.withColumn("hour", hour(df.datetime))
    df = df.withColumn("day", dayofmonth(df.datetime))
    df = df.withColumn("week", weekofyear(df.datetime))
    df = df.withColumn("month", month(df.datetime))
    df = df.withColumn("year", year(df.datetime))
    df = df.withColumn("weekday", dayofweek(df.datetime))
    time_table = df.select("hour", "day", "week", "month", "year", "weekday")
    
    # TODO: write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data+"time_table.parquet")

    # TODO: read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    song_log = df.drop("year").join(song_df, df.artist == song_df.artist_name,'inner')
    # TODO: extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_log.select("sessionId","datetime","userId","level","song_id","artist_id","location","userAgent","year","month")

    # TODO: write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data+"songplays_table.parquet")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()