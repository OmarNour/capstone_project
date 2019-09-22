import datetime as dt
import configparser
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import StructType as R, StructField as Fld, DecimalType as Dsml, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as DateTime
import psycopg2
from capstone_DWH.sql_queries import create_dwh_tables, populate_dwh_tables, drop_dwh_tables


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

    countries_df = spark.read.csv('src_data/i94_sas_countries.txt', sep="=", header=True)
    sparkdf_to_db(countries_df, db, schema, "countries", "overwrite", user, pw)

    us_states_df = spark.read.csv('src_data/i94_sas_states.txt', sep="=", header=True)
    sparkdf_to_db(us_states_df, db, schema, "us_states", "overwrite", user, pw)

    us_cities_demographics = spark.read.csv("src_data/us-cities-demographics.csv", sep=";", header=True)
    sparkdf_to_db(us_cities_demographics, db, schema, "us_cities_demographics", "overwrite", user, pw)

    visa_category_dic = {'cat_code': [1, 2, 3],
                         'cat_desc': ['Business', 'Pleasure', 'Student']}
    visa_category_df = pd.DataFrame.from_dict(visa_category_dic)
    visa_category_df = spark.createDataFrame(visa_category_df)
    sparkdf_to_db(visa_category_df, db, schema, "visa_categories", "overwrite", user, pw)

    ports_df = spark.read.csv('src_data/i94_sas_ports.txt', sep="=", header=True)
    sparkdf_to_db(ports_df, db, schema, "ports", "overwrite", user, pw)

    port_mode_dic = {'mode_code': [1, 2, 3, 9],
                     'mode_desc': ['Air', 'Sea', 'Land', 'Not reported']}
    port_mode_df = pd.DataFrame.from_dict(port_mode_dic)
    port_mode_df = spark.createDataFrame(port_mode_df)
    sparkdf_to_db(port_mode_df, db, schema, "port_modes", "overwrite", user, pw)

    gender_dic = {'gender_code': ['M', 'F', 'O'],
                  'gender_desc': ['Male', 'Female', 'Others']}
    gender_df = pd.DataFrame.from_dict(gender_dic)
    gender_df = spark.createDataFrame(gender_df)
    sparkdf_to_db(gender_df, db, schema, "gender", "overwrite", user, pw)

    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    sp_sas_data = spark.read.parquet("src_data/i94_sas_data")
    sp_sas_data = sp_sas_data.withColumn("arrival_date", get_date(sp_sas_data.arrdate))
    sp_sas_data = sp_sas_data.withColumn("departure_date", get_date(sp_sas_data.depdate))
    sparkdf_to_db(sp_sas_data, db, schema, "i94", "overwrite", user, pw)


def load_dwh_tables(refresh_tables):
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

    if refresh_tables:
        for query in drop_dwh_tables:
            cur.execute(query)
            conn.commit()

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

    load_staging_tables()
    load_dwh_tables(False)


if __name__ == "__main__":
    main()