import os
from datetime import datetime

import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, udf
from pyspark.sql.types import DateType


def init_spark_connection(appname, sparkmaster, minio_url,
                          minio_access_key, minio_secret_key):
    """ Init Spark connection and set hadoop configuration to read
    data from MINIO.

    Args:
        appname: spark application name.
        sparkmaster: spark master url.
        minio_url: an url to access to MINIO.
        minio_access_key: specific access key to MINIO.
        minio_secret_key: specific secret key to MINIO.

    Return:
         sc: spark connection object
    """
    sc = SparkSession \
        .builder \
        .appName(appname) \
        .master(sparkmaster) \
        .config("spark.jars.packages",'org.apache.hadoop:hadoop-aws:3.1.2,com.amazonaws:aws-java-sdk:1.11.534')\
        .getOrCreate()

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", minio_url)
    hadoop_conf.set("fs.s3a.access.key", minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
    return sc


def extract(sc, bucket_name, raw_data_path,section):
    """ Extract csv files from Minio.

    Args:
        sc: spark connection object.
        bucket_name: name of specific bucket in minio that contain data.
        raw_data_path: a path in bucket name that specifies data location.

    Return:
        df: raw dataframe.
    """

    if section == 'user':
        df = sc.read.json("/home/tapsi/niloo/express/ZerOne - Data Engineering Take Home-20220208T153634Z-001/"
                        "ZerOne - Data Engineering Take Home/Question2/users_data/users*.json")
    elif section == "tweet":
        df = sc.read.json("/home/tapsi/niloo/express/ZerOne - Data Engineering Take Home-20220208T153634Z-001/"
                        "ZerOne - Data Engineering Take Home/Question2/tweets_data/tweets*.json")
    else:
        print("unknown input")
    # return sc.read.csv('s3a://' + os.path.os.path.join(bucket_name,
    #                                                    raw_data_path),
    #                    header=True)

    return df


date_converter = udf(lambda z: datetime.strptime(z,'%a %b %d %H:%M:%S %z %Y'),DateType())


def user_df_transform(df):

    users = df\
        .select("message.*", "timestamp")\
        .select("id", "id_str", "name", "screen_name", "location", "description", "url", "protected", "followers_count",
                "friends_count","listed_count", "created_at", "favourites_count", "statuses_count", "lang",
                "profile_image_url_https", "timestamp")\
        .dropDuplicates(["id"])

    users = users\
        .withColumn('description', regexp_replace(col("description"), " ", ""))\
        .withColumn('name', regexp_replace(col("name"), " ", ""))\
        .withColumn('location', regexp_replace(col("location"), " ", ""))\
        .withColumn('url', regexp_replace(col("url"), " ", ""))\
        .withColumn("created_at", date_converter(users.created_at))



    return users

def tweet_df_transform(df):

    return df

def transform(df, section):
    """ Transform dataframe to an acceptable form.

    Args:
        df: raw dataframe

    Return:
        df: processed dataframe
    """
    # todo: write the your code here
    if section == 'user':
        processed_df = user_df_transform(df)
    elif section == 'tweet':
        processed_df = tweet_df_transform(df)
    else:
        print("unknown input")

    return df


def load(df, bucket_name, processed_data_path):
    """ Load clean dataframe to MINIO.

    Args:
        df: a processed dataframe.
        bucket_name: the name of specific bucket in minio that contain data.
        processed_data_path: a path in bucket name that
            specifies data location.

    Returns:
         Nothing!
    """
    # todo: change this function if
    df.write.csv('s3a://' + os.path.os.path.join(bucket_name,
                                                 processed_data_path),
                 header=True)


@click.command('ETL job')
@click.option('--appname', '-a', default='ETL Task', help='Spark app name')
@click.option('--sparkmaster', default='local[3]',
              help='Spark master node address:port')
@click.option('--minio_url', default='http://localhost:9000',
              help='import a module')
@click.option('--minio_access_key', default='minio')
@click.option('--minio_secret_key', default='minio123')
@click.option('--bucket_name', default='twitterusers')
@click.option('--raw_data_path', default='')
@click.option('--processed_data_path', default='')
@click.option('--section', default='user')

def main(appname, sparkmaster, minio_url,
         minio_access_key, minio_secret_key,
         bucket_name, raw_data_path, processed_data_path, section):

    sc = init_spark_connection(appname, sparkmaster, minio_url,
                               minio_access_key, minio_secret_key)

    # extract data from MINIO
    df = extract(sc, bucket_name, raw_data_path, section)

    # transform data to desired form
    clean_df = transform(df, section)

    # load clean data to MINIO
    load(clean_df, bucket_name, processed_data_path)


if __name__ == '__main__':
    main()