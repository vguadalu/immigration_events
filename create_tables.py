from pyspark.sql import types as T
from pyspark.sql.functions import udf, dayofmonth, dayofweek, month, year, weekofyear, col, date_format, when
from datetime import datetime

def create_fact_table(df,i94port_df, output_location):
    immigration_df = df.join(i94port_df, ['i94port'], how='full').select('visapost','arrdate', 'depdate', 
                                                                         'i94mode', col('cicid').alias('id'), 
                                                                         col('City').alias('City'), 'State Code',
                                                                         'i94port','count','visatype', 
                                                                         'entdepa','entdepd', 'entdepu','admnum')
    partition_columns = ['i94mode', 'visapost']
    immigration_df.write.parquet(output_location, partitionBy=partition_columns, mode="overwrite")
    
    return immigration_df

    
def create_calendar_table(df, date_column, output_location):
    calendar_df = df.select(date_column).distinct()

    calendar_df = calendar_df.withColumn('year', year(date_column))
    calendar_df = calendar_df.withColumn('month', month(date_column))
    calendar_df = calendar_df.withColumn('day', dayofmonth(date_column))
    calendar_df = calendar_df.withColumn('week', weekofyear(date_column))
    calendar_df = calendar_df.withColumn('weekday', date_format(date_column,'E'))
    
    partition_columns = ['year', 'month', 'week']
    calendar_df.write.parquet(output_location, partitionBy=partition_columns, mode="overwrite")
    
    return calendar_df
    
def create_demographics_table(df, output_location):
    demo_df = df.withColumn('Male Population', col('Male Population').cast('int'))
    demo_df = demo_df.withColumn('Female Population', col('Female Population').cast('int'))
    demo_df = demo_df.withColumn('Total Population', col('Total Population').cast('int'))
    demo_df = demo_df.withColumn('Foreign-born', col('Foreign-born').cast('int'))
    demo_df = demo_df.select('City', 'State Code', 'State', 'Male Population', 'Female Population', 
                             'Total Population', 'Foreign-born', 'Race')
    
    partition_columns = ['Race','State Code', 'City']
    demo_df.write.parquet(output_location, partitionBy=partition_columns, mode="overwrite")
    
    return demo_df
    
def create_city_temps_table(df, i94port_df, output_location):
    temps_df = df.join(i94port_df, ['City'], how='left')
    temps_df = temps_df.withColumn('AverageTemperature', col('AverageTemperature').cast('double'))
    temps_df = temps_df.select(col('dt').alias('Date'), 'City', 'Country', 'State Code', 
                               'AverageTemperature', 'Latitude', 'Longitude')
    
    partition_columns = ['State Code', 'City']
    temps_df.write.parquet(output_location, partitionBy=partition_columns, mode="overwrite")
    
    return temps_df

def _convert_year(year):
    current_year = int(datetime.now().strftime('%Y'))
    year = int(year)
    age = current_year - year
    return age
    
def create_immigrants_table(df, output_location):
    iimmigrants_df = df.select(col('cicid').alias('id'), 'gender', 
                               col('biryear').alias('age'), 
                               col('i94visa').alias('visa'), 'occup')

    udf_age_converter = udf(lambda x: _convert_year(x), T.IntegerType())
    immigrants_df = immigrants_df.withColumn('age', udf_age_converter('age'))

    partition_columns = ['gender']
    immigrants_df.write.parquet(output_location, partitionBy=partition_columns, mode="overwrite")
    
    return immigrants_df
    