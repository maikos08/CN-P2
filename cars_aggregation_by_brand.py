import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max


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


# Agregación por marca
print("Aggregating by brand...")
brand_agg = df.groupBy("brand").agg(
    count("id").alias("total_vehicles"),
    avg("milage").alias("avg_mileage"),
    spark_min("milage").alias("min_mileage"),
    spark_max("milage").alias("max_mileage")
).orderBy("brand")


print(f"Aggregated records: {brand_agg.count()}")
brand_agg.show(10)


# Escribir resultado en Parquet PARTICIONADO por brand
print(f"Writing to {args['output_path']} partitioned by brand...")
brand_agg.write.mode("overwrite").partitionBy("brand").parquet(args['output_path'])


print("Job completed successfully.")
job.commit()
