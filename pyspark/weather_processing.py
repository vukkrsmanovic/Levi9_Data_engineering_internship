import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws, trim, to_timestamp, date_format

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("WeatherProcessing") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    ) \
    .getOrCreate()

local_path = "sample_weather"
df_raw = spark.read.text(local_path)

# Prvi red je header
header_line = df_raw.first()[0]

# Ostali redovi su podaci
df_data = df_raw.filter(col("value") != header_line)

df_split = df_data.select(split(col("value"), ",").alias("parts"))

# Raspored:
# parts[0], parts[1], parts[2] -> komadi imena (sa zarezima unutra)
# parts[4] -> "2022-05-16 02:03:00" (datum + vreme)
# parts[10] -> temperatura
# parts[16] -> brzina vetra u m/s
#
# (ovo je izvedeno iz sample_weather koji si skinuo)

df_parsed = df_split.select(
    # name = tri komada spojena nazad u jedan string, pa trim
    trim(concat_ws(", ", col("parts")[0], col("parts")[1], col("parts")[2])).alias("name"),

    # datetime kao timestamp
    to_timestamp(col("parts")[4], "yyyy-MM-dd HH:mm:ss").alias("datetime_ts"),

    # temperatura (float/double)
    col("parts")[10].cast("double").alias("temperature"),

    # wind speed: iz m/s u km/h ( * 3.6 )
    (col("parts")[16].cast("double") * 3.6).alias("wind_speed_kmh")
)

# datetime u ISO 8601 string (bez time zone)
df_final = df_parsed.select(
    col("name"),
    date_format(col("datetime_ts"), "yyyy-MM-dd'T'HH:mm:ss").alias("datetime_iso"),
    col("temperature"),
    col("wind_speed_kmh")
)

# Snimi lokalno kao CSV sa ; delimiterom
output_path = "./weather_output_csv"

df_final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv(output_path)

print(f"Gotovo. Rezultat je u folderu: {output_path}")

spark.stop()
