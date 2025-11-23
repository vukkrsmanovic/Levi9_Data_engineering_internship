import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, to_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

INPUT_PATH = args['INPUT_PATH']
OUTPUT_PATH = args['OUTPUT_PATH']

# 1. LOAD CSV
df = (
    spark.read
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "false")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
)

print("Rows loaded:", df.count())

# 2. DATE COLUMN
df = df.withColumn(
    "date",
    to_date(to_timestamp(col("time_date"), "yyyy-MM-dd HH:mm:ss"))
)

# 3. WRITE PARQUET
(
    df
      .repartition("date")
      .write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("date")
      .save(OUTPUT_PATH)
)

job.commit()
print("DONE.")
