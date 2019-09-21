import datetime as dt
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StructType as R, StructField as Fld, DecimalType as Dsml, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as DateTime
import psycopg2
from capstone_DWH.sql_queries import create_dwh_tables, populate_dwh_tables


def sparkdf_to_db(pyspark_df, db, schema, table, mode, user, pw):
    pyspark_df.write.jdbc("jdbc:postgresql:{}".format(db),
                          "{}.{}".format(schema, table),
                          mode="{}".format(mode),
                          properties={"user": "{}".format(user),
                                      "password": "{}".format(pw)})


def load_staging_tables():
    """
    to load staging tables from S3 bucket
    :param cur: an instance to execute database commands
    :param conn: a connection object that creates a connection to database
    :return: none
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    db = config['STAGING_DB']['DB_NAME']
    schema = config['STAGING_DB']['DB_SCHEMA_NAME']
    user = config['STAGING_DB']['DB_USER']
    pw = config['STAGING_DB']['DB_PASSWORD']

    spark = SparkSession.builder.appName("capstone_project").getOrCreate()

    us_cities_demographics = spark.read.csv("src_data/us-cities-demographics.csv", sep=";", header=True)
    sparkdf_to_db(us_cities_demographics, db, schema, "us_cities_demographics", "overwrite", user, pw)

    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    sp_sas_data = spark.read.parquet("src_data/i94_sas_data")
    sp_sas_data = sp_sas_data.withColumn("arrival_date", get_date(sp_sas_data.arrdate))
    sp_sas_data = sp_sas_data.withColumn("departure_date", get_date(sp_sas_data.depdate))
    sparkdf_to_db(sp_sas_data, db, schema, "i94", "overwrite", user, pw)


def load_dwh_tables():
    """
    to load fact and dimension tables from staging tables
    :param cur: an instance to execute database commands
    :param conn: a connection object that creates a connection to database
    :return: none
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    host = config['DWH_DB']['DB_HOST']
    db = config['DWH_DB']['DB_NAME']
    user = config['DWH_DB']['DB_USER']
    pw = config['DWH_DB']['DB_PASSWORD']
    port = config['DWH_DB']['DB_PORT']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db, user, pw, port))
    cur = conn.cursor()

    for query in create_dwh_tables:
        cur.execute(query)
        conn.commit()

    for query in populate_dwh_tables:
        cur.execute(query)
        conn.commit()
    conn.close()


def main():
    """
    main method that calls load_staging_tables then insert_tables
    :return: none
    """

    # load_staging_tables()
    load_dwh_tables()


if __name__ == "__main__":
    main()