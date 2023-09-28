import pyspark
import requests
import json
import matplotlib.pyplot as plt
import os
from pyspark.sql import SparkSession
from transform.transformer import Transformer
from transform.loader import Loader
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.config import api_key


# Keeping it public - to use it in UDF
def read_weather_api(lat, lng):
    weather_url = 'https://api.openweathermap.org/data/2.5/weather'

    # Accessing api key from enviroment variable
    # api_key = os.getenv("API_KEY")  
    
    # Accessing api key through config file
    params = {
        'lat': lat,
        'lon': lng,
        'appid': api_key
        }

    response = requests.get(url=weather_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            'temperature': data['main']['temp'],
            'weather_conditions': data['weather'][0]['description'],
            }
    else:
        return None


if __name__ == '__main__':
    app_name = 'challenge'
    sales_data_path  = 'data/sales_data.csv'

    # creating spark session
    spark = SparkSession\
            .builder\
            .appName(app_name)\
            .getOrCreate()
  
    # creating objects
    transformer = Transformer(spark)
    loader = Loader(spark)
    
    # reading data
    sales = transformer.read_csv_data(sales_data_path)
    users = transformer.read_user_api()
    
    # extracting lat, long values from location of users
    users = transformer.extract_lat_long(users, 'location')
    users = transformer.drop_column(users, 'location')
 
    # making user defined function becuase api call will be made for each record
    weather_ud = udf(read_weather_api, MapType(StringType(), StringType()))
    users = users.withColumn("weather", weather_ud(users.lat, users.lng))

     # extracting temperatue and weather description from weather
    users = transformer.extract_weatherconditions(users)

    # dropping weather column to remove duplication 
    users = transformer.drop_column(users, 'weather')
    
    # joining sales and user data
    merged_data = transformer.merge(sales, users, sales.customer_id, users.id, "left")
    merged_data = transformer.drop_column(merged_data, 'id')
   
    # merged_data.show(truncate = False)
    merged_data.printSchema()

    # list of analytics dataframes
    analytics = transformer.analytics(merged_data)

    #saving it to Database
    loader.write_to_table(data=users, table='customers')
    loader.write_to_table(data=sales, table='sales')
    # loader.write_to_table(data=merged_data, table='merged')

    # stoping the spark session
    spark.stop()