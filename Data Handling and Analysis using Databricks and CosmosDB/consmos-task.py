!pip install azure.cosmos 
!pip install pyspark==3.4.0

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, count, array_contains, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from azure.cosmos import CosmosClient, PartitionKey


# Constants
URL = ""
KEY = ""

DB_NAME = "Lab2CosmosDB"
CONTAINER_NAME = "Lab2Container"

DATA_JSON_PATH = ""


client = CosmosClient(URL, credential=KEY)
database = client.create_database_if_not_exists(id=DB_NAME)
container = database.create_container_if_not_exists(
    id=CONTAINER_NAME,
    partition_key=PartitionKey(path="/category"),
    offer_throughput=400
)

print("Setup complete - Database and Container are ready")


spark = SparkSession.builder\
    .appName("AzureCosmosDBSpark") \
    .config("spark.jars.packages", "com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:4.30.0") \
    .getOrCreate()


schema = StructType([
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("cast", ArrayType(StringType()), True),
    StructField("genres", ArrayType(StringType()), True),
    StructField("href", StringType(), True),
    StructField("extract", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("thumbnail_width", IntegerType(), True),
    StructField("thumbnail_height", IntegerType(), True)
])

df = spark.read.format("json")\
          .option("multiline", True)\
          .schema(schema)\
          .load(DATA_JSON_PATH)

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
null_counts.show()


df = df.na.drop()
df = df.dropDuplicates(["title", "year"])

threshold = 1950
df = df.withColumn("category", when(df["year"] < threshold, "Classic").otherwise("Modern"))
df.show()


classic_films_df = df.filter(df["category"] == "Classic")
classic_films_df.show()

classic_silent_films_df = df.filter((df["category"] == "Classic") & array_contains(df["genres"], "Silent"))
classic_silent_films_df.show()

classic_silent_films_df = classic_silent_films_df.withColumn("id", monotonically_increasing_id().cast("string"))

cosmos_endpoint = URL
cosmos_master_key = KEY
cosmos_database_name = DB_NAME
cosmos_container_name = CONTAINER_NAME

config = {
    "spark.cosmos.accountEndpoint": cosmos_endpoint,
    "spark.cosmos.accountKey": cosmos_master_key,
    "spark.cosmos.database": cosmos_database_name,
    "spark.cosmos.container": cosmos_container_name,
    "spark.cosmos.write.strategy": "ItemOverwrite"
}

classic_silent_films_df.write \
  .format("cosmos.oltp") \
  .options(**config) \
  .mode("append") \
  .save()

df_cosmos = spark.read \
    .format("cosmos.oltp") \
    .options(**config) \
    .load()

df_cosmos.printSchema()
df_cosmos.columns

df_cosmos.createOrReplaceTempView("cosmos_data_view")
spark.sql("SELECT * FROM cosmos_data_view WHERE category = 'Classic'").show()


spark.stop()
