import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    artists_table = 
    
    # TODO: write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    # TODO: get filepath to log data file
    log_data =

    # TODO: read log data file
    df = 
    
    # TODO: filter by actions for song plays
    df = 

    # TODO: extract columns for users table    
    artists_table = 
    
    # TODO: write users table to parquet files
    artists_table

    # TODO: create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # TODO: create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # TODO: extract columns to create time table
    time_table = 
    
    # TODO: write time table to parquet files partitioned by year and month
    time_table

    # TODO: read in song data to use for songplays table
    song_df = 

    # TODO: extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # TODO: write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
