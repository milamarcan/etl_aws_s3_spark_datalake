import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

# Use these variables only when your data is in S3
#os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"]['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Spark session to process the data

    Arguments: None

    Returns:
        spark: a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Load JSON input data (song data part), extract 'songs' and 'artists' tables, store the data to parquet files. 

    Arguments: 
    - spark: a spark session
    - input_data: path to input data location (song data files)
    - output_data: path where to store output (parquet files)

    Returns: 
    - songs_table: directory with songs parquet files that were created during ETL process
    - artists_table: directory with artists parquet files that were created during ETL process
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # --- SONGS: SONGS TABLE ---
    # extract columns to create songs table
    songs_table = df.createOrReplaceTempView("df_songs_table")
    songs_table = spark.sql("""
    select song_id, title, artist_id, year, duration from df_songs_table order by title
""")

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = output_data + "songs_table.parquet"
    songs_table.write.mode("overwrite").partitionBy(
        "year", "artist_id").parquet(songs_table_path)

    # --- SONGS: ARTISTS TABLE ---
    # extract columns to create artists table
    df.createOrReplaceTempView("df_artists_table")
    artists_table = spark.sql("""
        SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude FROM df_artists_table order by name desc
    """)

    # write artists table to parquet files
    artists_table_path = output_data + "artists_table.parquet"
    artists_table.write.mode("overwrite").parquet(artists_table_path)

    return songs_table, artists_table


def process_log_data(spark, input_data, output_data):
    """Extract logs input data, transforms it into users, time and songplays tables, and writes the output as parquet files into the provided output location

    Arguments: 
    - spark: a Spark session
    - input_data: folder with log data
    - output_data: folder where processed data should be stored (in parquet format)

    Returns: 
    - users_table: directory with users parquet files. Stored in provided output location. 
    - time_table: directory with time parquet files. Stored in provided output location.
    - songplays_table: directory with songplays parquet files. Stored in provided output location.

    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df_filtered = df.filter(df.page == "NextSong")

    # --- LOGS: USERS TABLE ---
    # extract columns for users table
    df_filtered.createOrReplaceTempView("df_users_table")
    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level FROM df_users_table ORDER BY last_name
    """)

    # write users table to parquet files
    users_table_path = output_data + "users_table.parquet"
    users_table.write.mode("overwrite").parquet(users_table_path)

    # --- LOGS: TIME TABLE ---
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(
        x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_filtered = df_filtered.withColumn('timestamp', get_timestamp("ts"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_filtered = df_filtered.withColumn("datetime", get_datetime("ts"))

    # extract columns to create time table
    df_filtered.createOrReplaceTempView("df_time_table")
    time_table = spark.sql("""
        SELECT DISTINCT datetime AS start_time, 
        hour(timestamp) AS hour,
        day(timestamp) AS day,
        weekofyear(timestamp) AS week,
        month(timestamp) AS month,
        year(timestamp) AS year,
        dayofweek(timestamp) AS weekday 
        FROM df_time_table 
        ORDER BY start_time

    """)

    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "time_table.parquet"
    time_table.write.mode("overwrite").partitionBy(
        "year", "month").parquet(time_table_path)

    # --- ALL TOGETHER: SONGPLAYS TABLE ---
    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    df_songs = spark.read.json(song_data)

    # join song_df and log_df
    song_log_joined_df = df_filtered.join(df_songs, (df_filtered.song == df_songs.title)
                                          & (df_filtered.artist == df_songs.artist_name)
                                          & (df_filtered.length == df_songs.duration), how="inner")

    # extract columns from joined song and log datasets to create songplays table
    song_log_joined_df.createOrReplaceTempView("df_songplays_table")
    songplays_table = spark.sql("""
        SELECT 
            datetime AS start_time,
            userId AS user_id,
            level,
            sessionId AS session_id,
            location,
            userAgent AS user_agent,
            artist_id,
            song_id            
        FROM df_songplays_table
        ORDER BY (user_id, session_id)
    """)
    songplays_table = songplays_table.withColumn(
        "songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays_table.parquet"
    time_table.write.mode("overwrite").partitionBy(
        "year", "month").parquet(songplays_table_path)

    return users_table, time_table, songplays_table


def main():
    """Calls create spark session function, provide paths to input and output data, calls processing song and log data

    Arguments: None

    Returns: None
    """
    spark = create_spark_session()
    input_data = config["LOCAL"]["INPUT_DATA"]
    output_data = config["LOCAL"]["OUTPUT_DATA"]

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
