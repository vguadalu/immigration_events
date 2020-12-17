import os
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

import utils
import create_tables as ct

def create_spark_session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY):
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY) \
        .getOrCreate()
    
    return spark

def process_immigration_data(spark, immigration_file,i94port_df, i94addr_df, output_location):
    immigration_df =spark.read.format('com.github.saurfang.sas.spark').load(immigration_file)

    immigration_df = utils.clean_immigration_data(immigration_df, i94port_df, i94addr_df)
    immigration_df = utils.manipulate_immigration_data(immigration_df)
    
    arrivals_table = ct.create_calendar_table(immigration_df,'arrdate', f'{output_location}/arrival_dates')
    departures_table = ct.create_calendar_table(immigration_df, 'depdate', f'{output_location}/departure_dates')
    
    immigrants_table = ct.create_immigrants_table(immigration_df, f'{output_location}/immigrants')
    immigrations_table = ct.create_fact_table(immigration_df, i94port_df, f'{output_location}/imigration')
    

def process_temperature_data(spark, temperature_file, i94port_df, output_location):
    temp_df = spark.read.format('csv').option('header', 'true').load(temperature_file)
    
    temperature_df  = utils.clean_temperature_data(temp_df)
    city_temps_table = ct.create_city_temps_table(temperature_df, i94port_df, f'{output_location}/city_temps')
    
def process_demographics_data(spark, usa_demographics_file, output_location):
    demogrpahics_df = spark.read.option('delimiter', ';').format('csv').option('header', 'true').load(usa_demographics_file)
    
    demogrpahics_df = utils.clean_demographics_data(demogrpahics_df)    
    demographics_table = ct.create_demographics_table(demogrpahics_df, f'{output_location}/demographics')
    
def main():
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''

    spark = create_spark_session(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    s3 = "s3a://vegc-capstone-data"
    
    immigration_file = 'i94_dec16_sub.sas7bdat'
    temperature_file = 'GlobalLandTemperaturesByCity.csv'
    usa_demographics_file = 'us-cities-demographics.csv'
    i94_port_file = 'valid_i94_ports.csv'
    i94_addr_file = 'valid_i94addr_codes.csv'

    i94port_df = spark.read.format('csv').option('header', 'true').load(i94_port_file)
    i94port_df = utils.clean_i94port_data(i94port_df)
    i94addr_df = spark.read.format('csv').option('header', 'true').load(i94_addr_file)
    
    process_temperature_data(spark, temperature_file, i94port_df, f'{s3}/parquets/')
    process_demographics_data(spark, usa_demographics_file, f'{s3}/parquets/')
    process_immigration_data(spark, immigration_file, i94port_df, i94addr_df, f'{s3}/parquets/')

    
if __name__ == '__main__':
    main()
