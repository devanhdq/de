from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('sofascore') \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/sofascore") \
    .config("spark.mongodb.spark.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

tournaments_raw_df = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("uri", 'mongodb://localhost:27017/sofascore.tournaments') \
    .load()

tournaments_raw_df.show()
