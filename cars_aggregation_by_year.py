import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max, countDistinct


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database', 'table_name', 'output_path'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Leer desde Glue Catalog
print(f"Reading from {args['database']}.{args['table_name']}...")
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args['database'],
    table_name=args['table_name']
)


# Convertir a DataFrame
df = dynamic_frame.toDF()
print(f"Records read: {df.count()}")


# Agregación por año
print("Aggregating by year...")
year_agg = df.groupBy("model_year").agg(
    count("id").alias("total_vehicles"),
    countDistinct("brand").alias("unique_brands"),
    avg("milage").alias("avg_mileage"),
    spark_min("milage").alias("min_mileage"),
    spark_max("milage").alias("max_mileage")
).orderBy(col("model_year").desc())


print(f"Aggregated records: {year_agg.count()}")
year_agg.show(15)


# Escribir resultado en Parquet PARTICIONADO por model_year
print(f"Writing to {args['output_path']} partitioned by model_year...")
year_agg.write.mode("overwrite").partitionBy("model_year").parquet(args['output_path'])


print("Job completed successfully.")
job.commit()
