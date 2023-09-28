import pyspark
import os
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class Transformer:
    def __init__(self, spark) -> None:
        self.spark = spark
        self.user_data_url = 'https://jsonplaceholder.typicode.com/users'

    def read_csv_data(self, path):
        data = self.spark.read.option("header",True).option("inferSchema", True).csv(path)
        data = data.withColumn("amount", (data.quantity * data.price).cast("double"))
        return data
    
    def read_user_api(self):
        response = requests.get(url = self.user_data_url)
        if response.status_code == 200:
            data = response.json()
            df = self.spark.createDataFrame(data).select(
                col("id"),
                col("name"),
                col("email"),
                col("username"),
                col("address")['geo'].alias("location"),
                col("address")['city'].alias("city")
                )
            
            return df
        else:
            None
        
    def merge(self, df1, df2, a, b, join):
        final_df = df1.join(df2, a == b, join)
        return final_df
    
    def extract_lat_long(self, df, column):
        lat_expr = r'lat=([-+]?\d*\.\d+|\d+)'
        lng_expr = r'lng=([-+]?\d*\.\d+|\d+)'

        df =  df.withColumn("lat", regexp_extract(col(column), lat_expr, 1).cast("double"))\
                .withColumn("lng", regexp_extract(col(column), lng_expr, 1).cast("double"))
        return df
    
    def extract_weatherconditions(self, df):
        df =  df.withColumn("temperature", df.weather.temperature.cast("double"))\
                .withColumn("weather_condition", df.weather.weather_conditions)
        return df

    def drop_column(self, df, column):
        df = df.drop(column)
        return df
    
    def analytics(self, data):
        total_sales         =   data.groupBy("customer_id")\
                                    .agg(sum("amount").alias("total_shopping"))\
                                    .toPandas()

        total_sales.plot.line(x='customer_id', y='total_shopping', rot=1)
        plt.savefig('visualizations/total_sales_by_each_customer.png')
        
        avg_order_quantity  =   data.groupBy("product_id")\
                                    .agg(avg("quantity").alias("avg_quantity")) \
                                    .sort(desc("avg_quantity"))
       
        top_selling_product =   data.groupBy("product_id")\
                                    .agg(sum("quantity").alias("total_quantity_sold")) \
                                    .sort(desc("total_quantity_sold"))

        top_customer        =   data.groupBy("customer_id")\
                                    .agg(sum("price").alias("total_shopping")) \
                                    .sort(desc("total_shopping"))

        avg_sales_weather   =   data.groupBy("weather_condition")\
                                    .agg(sum("price").alias("total_sales")) \
                                    .sort(desc("total_sales"))\
                                    .toPandas()
        
        fig, ax = plt.subplots()
        ax.pie(avg_sales_weather.total_sales, labels=avg_sales_weather.weather_condition, autopct='%1.1f%%')
        plt.savefig('visualizations/sales_in_different_weather.png')

        monthly_sales       =   data.groupBy(date_format(data.order_date,'MMM/yyyy').alias("month"))\
                                .agg(sum("price").alias("total_sales"))\
                                .toPandas()

        monthly_sales.plot.line(x='month', y='total_sales', rot=1)
        plt.savefig('visualizations/monthly_sales.png')

        quarter_data        =   data.withColumn("quarter", quarter(col("order_date")))
        quarterly_sales     =   quarter_data.groupBy("quarter", year("order_date").alias('quarter_year'))\
                                            .agg(sum("price").alias("total_sales"))\
                                            .orderBy("quarter_year")\
                                            .toPandas()

        quarterly_sales.plot.bar(x='quarter_year', y='total_sales', rot=1)
        plt.savefig('visualizations/quarterly_sales.png')

        return ([
            total_sales, 
            avg_order_quantity, 
            top_selling_product, 
            top_customer, 
            avg_sales_weather,
            monthly_sales,
            quarterly_sales
            ])
    

