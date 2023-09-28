from config.config import postgres_url, properties, schema

class Loader:
    def __init__(self, spark) -> None:
        self.spark = spark

    def write_to_table(self, data, table):
        table = schema+'.'+table
        data.write.jdbc(postgres_url, table, mode="append", properties=properties)