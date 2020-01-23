from pyspark.sql import SparkSession

spark_session = SparkSession \
    .builder \
    .appName("PoD - Gerador de DataProfile (PDF)") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
